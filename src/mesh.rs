use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use tokio::process::Command;
use tokio::sync::RwLock;

// ── Constants ──────────────────────────────────────────────────────────

/// Default mesh nodes. Override via MESHD_NODES env var (comma-separated "name=ip" pairs)
/// or via [machine.peers] in meshd.toml.
pub fn mesh_nodes() -> Vec<(&'static str, &'static str)> {
    // In production, read from config. These are examples.
    vec![
        ("node1", "100.64.0.1"),
        ("node2", "100.64.0.2"),
    ]
}

pub const RATE_LIMIT_SECONDS: u64 = 10;
pub const MAX_PROMPT_SIZE: usize = 10 * 1024;

// ── Types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaneInfo {
    pub pid: String,
    pub tty: String,
    pub command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshNodeStatus {
    pub machine: String,
    pub ip: String,
    pub healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshTask {
    pub id: String,
    pub prompt: String,
    pub from: String,
    pub created: String,
    #[serde(default)]
    pub claimed_by: Option<String>,
}

// ── Rate Limiter ───────────────────────────────────────────────────────

pub struct RateLimiter {
    map: RwLock<HashMap<String, Instant>>,
}

impl RateLimiter {
    pub fn new() -> Self {
        RateLimiter {
            map: RwLock::new(HashMap::new()),
        }
    }

    /// Returns Ok(()) if the key is allowed, Err with seconds remaining otherwise.
    pub async fn check(&self, key: &str) -> std::result::Result<(), u64> {
        let now = Instant::now();
        let mut map = self.map.write().await;
        if let Some(last) = map.get(key) {
            let elapsed = now.duration_since(*last).as_secs();
            if elapsed < RATE_LIMIT_SECONDS {
                return Err(RATE_LIMIT_SECONDS - elapsed);
            }
        }
        map.insert(key.to_string(), now);
        Ok(())
    }
}

// ── Inject Tmux ────────────────────────────────────────────────────────

/// Inject a prompt into a tmux session. If machine is local, use tmux directly.
/// If remote, SCP a temp file and SSH to run tmux commands.
pub async fn inject_tmux(machine: &str, prompt: &str, session: &str) -> Result<String> {
    if prompt.len() > MAX_PROMPT_SIZE {
        return Err(anyhow!(
            "prompt too large: {} bytes (max {})",
            prompt.len(),
            MAX_PROMPT_SIZE
        ));
    }

    let local_machine = get_local_machine_id();

    if machine == &local_machine || machine == "local" {
        // Local injection: tmux load-buffer + paste-buffer + send-keys Enter
        let tmp = format!("/tmp/mesh-inject-{}.txt", std::process::id());
        tokio::fs::write(&tmp, prompt).await?;

        let load = Command::new("/opt/homebrew/bin/tmux")
            .args(["load-buffer", &tmp])
            .output()
            .await?;
        if !load.status.success() {
            return Err(anyhow!(
                "tmux load-buffer failed: {}",
                String::from_utf8_lossy(&load.stderr)
            ));
        }

        let paste = Command::new("/opt/homebrew/bin/tmux")
            .args(["paste-buffer", "-t", session])
            .output()
            .await?;
        if !paste.status.success() {
            return Err(anyhow!(
                "tmux paste-buffer failed: {}",
                String::from_utf8_lossy(&paste.stderr)
            ));
        }

        let enter = Command::new("/opt/homebrew/bin/tmux")
            .args(["send-keys", "-t", session, "Enter"])
            .output()
            .await?;
        if !enter.status.success() {
            return Err(anyhow!(
                "tmux send-keys failed: {}",
                String::from_utf8_lossy(&enter.stderr)
            ));
        }

        let _ = tokio::fs::remove_file(&tmp).await;
        Ok(format!("injected locally into session '{}'", session))
    } else {
        // Remote: find IP, SCP + SSH
        let ip = resolve_machine_ip(machine)?;
        let tmp_local = format!("/tmp/mesh-inject-{}.txt", std::process::id());
        let tmp_remote = format!("/tmp/mesh-inject-remote-{}.txt", std::process::id());
        tokio::fs::write(&tmp_local, prompt).await?;

        let scp = Command::new("scp")
            .args(["-o", "StrictHostKeyChecking=no", &tmp_local, &format!("{}:{}", ip, tmp_remote)])
            .output()
            .await?;
        if !scp.status.success() {
            return Err(anyhow!(
                "scp failed: {}",
                String::from_utf8_lossy(&scp.stderr)
            ));
        }

        let ssh_cmd = format!(
            "tmux load-buffer {} && tmux paste-buffer -t {} && tmux send-keys -t {} Enter && rm -f {}",
            tmp_remote, session, session, tmp_remote
        );
        let ssh = Command::new("ssh")
            .args(["-o", "StrictHostKeyChecking=no", &ip, &ssh_cmd])
            .output()
            .await?;
        if !ssh.status.success() {
            return Err(anyhow!(
                "ssh tmux failed: {}",
                String::from_utf8_lossy(&ssh.stderr)
            ));
        }

        let _ = tokio::fs::remove_file(&tmp_local).await;
        Ok(format!(
            "injected remotely into {}:{} session '{}'",
            machine, ip, session
        ))
    }
}

// ── Relay ──────────────────────────────────────────────────────────────

/// Forward an HTTP request to another machine's meshd.
pub async fn relay(
    machine: &str,
    path: &str,
    body: serde_json::Value,
    auth_token: &str,
) -> Result<serde_json::Value> {
    let ip = resolve_machine_ip(machine)?;
    // Try meshd port (9451) first
    let url = format!("http://{}:9451{}", ip, path);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    let res = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", auth_token))
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow!("relay to {} failed: {}", machine, e))?;

    let status = res.status();
    let resp_body: serde_json::Value = res
        .json()
        .await
        .unwrap_or_else(|_| serde_json::json!({"error": "non-json response"}));

    if status.is_success() {
        Ok(resp_body)
    } else {
        Err(anyhow!(
            "relay to {} returned {}: {:?}",
            machine,
            status,
            resp_body
        ))
    }
}

// ── Broadcast ──────────────────────────────────────────────────────────

/// Fan-out a relay to all MESH_NODES except the excluded machine.
pub async fn broadcast(
    prompt: &str,
    exclude: &str,
    auth_token: &str,
) -> HashMap<String, serde_json::Value> {
    let mut results = HashMap::new();
    let mut handles = vec![];

    for (name, _ip) in mesh_nodes() {
        if name == exclude {
            continue;
        }
        let name = name.to_string();
        let body = serde_json::json!({ "prompt": prompt });
        let token = auth_token.to_string();

        handles.push(tokio::spawn(async move {
            let result = relay(&name, "/message", body, &token).await;
            (name, result)
        }));
    }

    for handle in handles {
        if let Ok((name, result)) = handle.await {
            match result {
                Ok(v) => {
                    results.insert(name, v);
                }
                Err(e) => {
                    results.insert(name, serde_json::json!({"error": e.to_string()}));
                }
            }
        }
    }

    results
}

// ── Discover Mesh ──────────────────────────────────────────────────────

/// Hit /health on all MESH_NODES, return status map.
pub async fn discover_mesh(port: u16) -> Vec<MeshNodeStatus> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .unwrap();

    let mut handles = vec![];

    for (name, ip) in mesh_nodes() {
        let client = client.clone();
        let name = name.to_string();
        let ip = ip.to_string();
        let port = port;

        handles.push(tokio::spawn(async move {
            let url = format!("http://{}:{}/health", ip, port);
            match client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let detail: Option<serde_json::Value> = resp.json().await.ok();
                    MeshNodeStatus {
                        machine: name,
                        ip,
                        healthy: true,
                        detail,
                    }
                }
                _ => MeshNodeStatus {
                    machine: name,
                    ip,
                    healthy: false,
                    detail: None,
                },
            }
        }));
    }

    let mut statuses = vec![];
    for handle in handles {
        if let Ok(status) = handle.await {
            statuses.push(status);
        }
    }
    statuses
}

// ── Read Pane ──────────────────────────────────────────────────────────

/// Read the last N lines from a tmux pane via capture-pane.
pub async fn read_pane(tty: &str, lines: usize) -> Result<String> {
    let output = Command::new("/opt/homebrew/bin/tmux")
        .args([
            "capture-pane",
            "-t",
            tty,
            "-p",
            "-S",
            &format!("-{}", lines),
        ])
        .output()
        .await?;

    if !output.status.success() {
        return Err(anyhow!(
            "tmux capture-pane failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

// ── Get Local Panes ────────────────────────────────────────────────────

/// Find Claude processes and parse their TTYs.
pub async fn get_local_panes() -> Result<Vec<PaneInfo>> {
    let output = Command::new("bash")
        .args(["-c", "ps aux | grep -E '[c]laude' || true"])
        .output()
        .await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut panes = vec![];

    for line in stdout.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 11 {
            let pid = parts[1].to_string();
            let tty = parts[6].to_string();
            let command = parts[10..].join(" ");
            // Only include if it looks like a real tty
            if tty != "?" && !tty.is_empty() {
                panes.push(PaneInfo { pid, tty, command });
            }
        }
    }

    Ok(panes)
}

// ── Task Queue ─────────────────────────────────────────────────────────

fn inbox_dir() -> PathBuf {
    let home = std::env::var("HOME").expect("HOME not set");
    PathBuf::from(home).join(".claude/state/mesh-inbox")
}

/// List all pending tasks in the inbox.
pub async fn list_tasks() -> Result<Vec<MeshTask>> {
    let dir = inbox_dir();
    if !dir.exists() {
        tokio::fs::create_dir_all(&dir).await?;
        return Ok(vec![]);
    }

    let mut tasks = vec![];
    let mut entries = tokio::fs::read_dir(&dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            if let Ok(content) = tokio::fs::read_to_string(&path).await {
                if let Ok(task) = serde_json::from_str::<MeshTask>(&content) {
                    tasks.push(task);
                }
            }
        }
    }

    tasks.sort_by(|a, b| a.created.cmp(&b.created));
    Ok(tasks)
}

/// Queue a new task into the inbox.
pub async fn queue_task(prompt: &str, from: &str) -> Result<MeshTask> {
    let dir = inbox_dir();
    tokio::fs::create_dir_all(&dir).await?;

    let id = format!(
        "{}-{}",
        chrono::Utc::now().format("%Y%m%d%H%M%S"),
        &uuid_short()
    );
    let task = MeshTask {
        id: id.clone(),
        prompt: prompt.to_string(),
        from: from.to_string(),
        created: chrono::Utc::now().to_rfc3339(),
        claimed_by: None,
    };

    let path = dir.join(format!("{}.json", id));
    let json = serde_json::to_string_pretty(&task)?;
    tokio::fs::write(&path, json).await?;

    Ok(task)
}

/// Claim the oldest unclaimed task.
/// Uses a lock file per task to prevent TOCTOU double-claim race.
pub async fn claim_task(claimer: &str) -> Result<Option<MeshTask>> {
    let dir = inbox_dir();
    if !dir.exists() {
        return Ok(None);
    }

    let mut entries = tokio::fs::read_dir(&dir).await?;
    let mut files: Vec<PathBuf> = vec![];
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            files.push(path);
        }
    }
    files.sort();

    for path in files {
        // Atomic lock: create_new fails if another process already holds the lock
        let lock_path = path.with_extension("lock");
        let lock_result = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path);
        if lock_result.is_err() {
            continue; // Another claimer holds this task's lock
        }

        let claimed = async {
            let content = tokio::fs::read_to_string(&path).await?;
            let mut task: MeshTask = serde_json::from_str(&content)?;
            if task.claimed_by.is_some() {
                return Ok::<Option<MeshTask>, anyhow::Error>(None); // Already claimed
            }
            task.claimed_by = Some(claimer.to_string());
            let json = serde_json::to_string_pretty(&task)?;
            tokio::fs::write(&path, json).await?;
            Ok(Some(task))
        }
        .await;

        // Clean up lock file regardless of outcome
        let _ = std::fs::remove_file(&lock_path);

        match claimed {
            Ok(Some(task)) => return Ok(Some(task)),
            Ok(None) => continue,    // Was already claimed
            Err(_) => continue,      // Parse/IO error, skip
        }
    }

    Ok(None)
}

// ── Helpers ────────────────────────────────────────────────────────────

fn resolve_machine_ip(machine: &str) -> Result<String> {
    for (name, ip) in mesh_nodes() {
        if name == machine {
            return Ok(ip.to_string());
        }
    }
    Err(anyhow!("unknown machine: {}", machine))
}

fn get_local_machine_id() -> String {
    // Read from meshd config or fall back to hostname
    if let Ok(contents) = std::fs::read_to_string(
        dirs::home_dir()
            .unwrap_or_default()
            .join(".meshd/meshd.toml"),
    ) {
        for line in contents.lines() {
            if line.starts_with("id") {
                if let Some(val) = line.split('=').nth(1) {
                    let id = val.trim().trim_matches('"');
                    if !id.is_empty() {
                        return id.to_string();
                    }
                }
            }
        }
    }
    "local".to_string()
}

fn uuid_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{:08x}", t)
}

// ── IP Allowlist Check ─────────────────────────────────────────────────

/// Check if a client IP is allowed for POST endpoints.
/// Allows: Tailscale (100.x.x.x), localhost (127.0.0.1, ::1)
pub fn is_ip_allowed(ip: &str) -> bool {
    if ip == "127.0.0.1" || ip == "::1" || ip == "localhost" {
        return true;
    }
    // Tailscale CGNAT range: 100.64.0.0/10 (100.64.x.x - 100.127.x.x)
    if let Some(first_octet) = ip.split('.').next() {
        if first_octet == "100" {
            if let Some(second_octet) = ip.split('.').nth(1) {
                if let Ok(n) = second_octet.parse::<u32>() {
                    return (64..=127).contains(&n);
                }
            }
        }
    }
    false
}
