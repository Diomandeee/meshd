use crate::slot::{Slot, SlotState};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

pub async fn run_watchdog(slots: Arc<HashMap<String, Arc<Slot>>>) {
    loop {
        sleep(Duration::from_secs(10)).await;

        for (name, slot) in slots.iter() {
            let state = slot.current_state().await;

            // Check if child process died unexpectedly
            if state != SlotState::Unborn && slot.is_dead().await {
                tracing::warn!(slot = %name, "child process died — restarting");
                if let Err(e) = slot.restart().await {
                    tracing::error!(slot = %name, error = %e, "restart failed");
                } else {
                    tracing::info!(slot = %name, gen = slot.gen(), "slot restarted");
                }
                continue;
            }

            // Idle timeout for lazy slots: kill after idle_timeout_secs
            if slot.config.lazy && state == SlotState::Ready {
                let idle = slot.idle_duration().await;
                if idle.as_secs() > slot.config.idle_timeout_secs {
                    tracing::info!(
                        slot = %name,
                        idle_secs = idle.as_secs(),
                        timeout = slot.config.idle_timeout_secs,
                        "idle timeout — killing slot"
                    );
                    slot.kill().await;
                }
            }

            // Warn if Busy for > 10 minutes with no output (stuck detection)
            if state == SlotState::Busy {
                let idle = slot.idle_duration().await;
                if idle.as_secs() > 600 {
                    tracing::warn!(
                        slot = %name,
                        stuck_secs = idle.as_secs(),
                        "slot has been Busy with no activity for 10+ minutes"
                    );
                }
            }
        }
    }
}
