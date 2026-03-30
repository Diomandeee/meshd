use crate::config::MachineConfig;
use crate::slot::{Slot, SlotState};
use anyhow::Result;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

pub struct Registry {
    pub machine: MachineConfig,
    pub supabase_key: String,
    pub slots: Arc<HashMap<String, Arc<Slot>>>,
    client: reqwest::Client,
}

impl Registry {
    pub fn new(
        machine: MachineConfig,
        supabase_key: String,
        slots: Arc<HashMap<String, Arc<Slot>>>,
    ) -> Self {
        Registry {
            machine,
            supabase_key,
            slots,
            client: reqwest::Client::new(),
        }
    }

    pub async fn boot_register(&self) -> Result<()> {
        for (slot_name, slot) in self.slots.iter() {
            let id = format!("{}:{}", self.machine.id, slot_name);
            let account_tier = slot_to_account_tier(&slot.config.env);
            let payload = json!({
                "id": id,
                "machine_id": self.machine.id,
                "slot_name": slot_name,
                "agent": slot.config.agent,
                "account_tier": account_tier,
                "status": "offline",
                "generation": 0,
                "last_seen": chrono::Utc::now().to_rfc3339(),
                "api_port": self.machine.api_port,
                "tailscale_ip": self.machine.tailscale_ip
            });

            let url = format!("{}/rest/v1/agent_slots", self.machine.supabase_url);
            let res = self
                .client
                .post(&url)
                .header("apikey", &self.supabase_key)
                .header("Authorization", format!("Bearer {}", &self.supabase_key))
                .header("Content-Type", "application/json")
                .header("Prefer", "resolution=merge-duplicates")
                .json(&payload)
                .send()
                .await;

            match res {
                Ok(r) if r.status().is_success() || r.status().as_u16() == 409 => {
                    tracing::info!(slot = %id, "registered in Supabase");
                }
                Ok(r) => {
                    tracing::warn!(slot = %id, status = %r.status(), "Supabase upsert non-success");
                }
                Err(e) => {
                    tracing::warn!(slot = %id, error = %e, "Supabase registration failed (will retry on heartbeat)");
                }
            }
        }
        Ok(())
    }

    /// Run the heartbeat loop every 60s indefinitely.
    pub async fn run_heartbeat(&self) {
        loop {
            sleep(Duration::from_secs(60)).await;
            if let Err(e) = self.heartbeat_once().await {
                tracing::warn!(error = %e, "heartbeat error");
            }
        }
    }

    async fn heartbeat_once(&self) -> Result<()> {
        tracing::info!(slots = self.slots.len(), "heartbeat tick");
        for (slot_name, slot) in self.slots.iter() {
            let id = format!("{}:{}", self.machine.id, slot_name);
            let state = slot.current_state().await;
            let status = match state {
                SlotState::Unborn => "offline",
                SlotState::Spawning => "spawning",
                SlotState::Ready => "ready",
                SlotState::Busy => "busy",
            };

            let url = format!(
                "{}/rest/v1/agent_slots?id=eq.{}",
                self.machine.supabase_url,
                urlencoding::encode(&id)
            );

            let payload = json!({
                "status": status,
                "generation": slot.gen(),
                "last_seen": chrono::Utc::now().to_rfc3339()
            });

            match self
                .client
                .patch(&url)
                .header("apikey", &self.supabase_key)
                .header("Authorization", format!("Bearer {}", &self.supabase_key))
                .header("Content-Type", "application/json")
                .json(&payload)
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => {
                    tracing::debug!(slot = %id, status = %status, "heartbeat ok");
                }
                Ok(r) => {
                    let code = r.status();
                    let body = r.text().await.unwrap_or_default();
                    tracing::warn!(slot = %id, status = %code, body = %body, "heartbeat PATCH failed");
                }
                Err(e) => {
                    tracing::warn!(slot = %id, error = %e, "heartbeat request error");
                }
            }
        }
        Ok(())
    }
}

fn slot_to_account_tier(env: &std::collections::HashMap<String, String>) -> Option<String> {
    env.get("CLAUDE_CONFIG_DIR").and_then(|dir| {
        let path = std::path::Path::new(dir);
        path.file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
    })
}
