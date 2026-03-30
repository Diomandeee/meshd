use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct MachineConfig {
    pub id: String,
    pub tailscale_ip: String,
    pub api_port: u16,
    pub supabase_url: String,
    /// Port for mesh-api compatibility (default: None = disabled)
    pub mesh_port: Option<u16>,
    /// Auth token override for mesh operations (falls back to MESHD_TOKEN env var)
    pub mesh_auth_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SlotConfig {
    pub name: String,
    pub agent: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub lazy: bool,
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
    #[serde(default = "default_rows")]
    pub rows: u16,
    #[serde(default = "default_cols")]
    pub cols: u16,
}

fn default_idle_timeout() -> u64 { 1800 }
fn default_rows() -> u16 { 50 }
fn default_cols() -> u16 { 220 }

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub machine: MachineConfig,
    #[serde(rename = "slots")]
    pub slots: Vec<SlotConfig>,
}

impl Config {
    pub fn load(path: &PathBuf) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config at {}", path.display()))?;
        let config: Config = toml::from_str(&contents)
            .with_context(|| "Failed to parse meshd.toml")?;
        Ok(config)
    }

    pub fn default_path() -> PathBuf {
        meshd_home().join("meshd.toml")
    }
}

fn meshd_home() -> PathBuf {
    let home = std::env::var("HOME").expect("HOME not set");
    PathBuf::from(home).join(".meshd")
}
