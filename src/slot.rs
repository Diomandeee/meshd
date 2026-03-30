use crate::config::SlotConfig;
use crate::pty::{spawn_slot, write_to_pty, PtyHandle, SpawnConfig};
use anyhow::Result;
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};

#[derive(Debug, Clone, PartialEq)]
pub enum SlotState {
    Unborn,
    Spawning,
    Ready,
    Busy,
}

impl std::fmt::Display for SlotState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlotState::Unborn => write!(f, "unborn"),
            SlotState::Spawning => write!(f, "spawning"),
            SlotState::Ready => write!(f, "ready"),
            SlotState::Busy => write!(f, "busy"),
        }
    }
}

pub struct Slot {
    pub config: SlotConfig,
    pub state: Arc<RwLock<SlotState>>,
    pub pty: Arc<RwLock<Option<PtyHandle>>>,
    pub generation: Arc<AtomicU64>,
    pub last_active: Arc<RwLock<Instant>>,
}

impl Slot {
    pub fn new(config: SlotConfig) -> Self {
        Slot {
            config,
            state: Arc::new(RwLock::new(SlotState::Unborn)),
            pty: Arc::new(RwLock::new(None)),
            generation: Arc::new(AtomicU64::new(0)),
            last_active: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Boot the slot process. No-op if already alive.
    pub async fn boot(&self) -> Result<()> {
        {
            let state = self.state.read().await;
            if *state != SlotState::Unborn {
                return Ok(());
            }
        }
        *self.state.write().await = SlotState::Spawning;

        let handle = spawn_slot(SpawnConfig {
            agent: &self.config.agent,
            args: &self.config.args,
            env: &self.config.env,
            rows: self.config.rows,
            cols: self.config.cols,
        })?;

        *self.pty.write().await = Some(handle);
        self.generation.fetch_add(1, Ordering::SeqCst);
        *self.state.write().await = SlotState::Ready;
        *self.last_active.write().await = Instant::now();

        tracing::info!(
            slot = %self.config.name,
            gen = self.generation.load(Ordering::SeqCst),
            "slot booted"
        );
        Ok(())
    }

    /// Inject a prompt. Boots lazily if needed.
    pub async fn inject(&self, prompt: &str) -> Result<u64> {
        // Lazy boot
        if *self.state.read().await == SlotState::Unborn {
            self.boot().await?;
        }

        {
            let pty = self.pty.read().await;
            let handle = pty.as_ref().ok_or_else(|| anyhow::anyhow!("no pty"))?;
            let mut payload = prompt.to_string();
            if !payload.ends_with('\n') {
                payload.push('\n');
            }
            write_to_pty(handle.master_fd, payload.as_bytes())?;
        }

        *self.state.write().await = SlotState::Busy;
        *self.last_active.write().await = Instant::now();

        Ok(self.generation.load(Ordering::SeqCst))
    }

    /// Subscribe to live output lines.
    pub async fn subscribe(&self) -> Option<broadcast::Receiver<String>> {
        let pty = self.pty.read().await;
        pty.as_ref().map(|h| h.tx.subscribe())
    }

    /// Return the last N lines from the ring buffer.
    pub async fn read_ring(&self, n: usize) -> Vec<String> {
        let pty = self.pty.read().await;
        match pty.as_ref() {
            None => vec![],
            Some(h) => {
                let ring = h.ring.lock().unwrap();
                let start = ring.len().saturating_sub(n);
                ring.iter().skip(start).cloned().collect()
            }
        }
    }

    /// True if the child process is dead.
    pub async fn is_dead(&self) -> bool {
        let pty = self.pty.read().await;
        match pty.as_ref() {
            None => false,
            Some(h) => {
                match waitpid(h.child_pid, Some(WaitPidFlag::WNOHANG)) {
                    Ok(WaitStatus::Exited(_, _)) | Ok(WaitStatus::Signaled(_, _, _)) => true,
                    _ => false,
                }
            }
        }
    }

    /// Kill current process and reset to Unborn (for lazy slots on idle timeout).
    pub async fn kill(&self) {
        let pty = self.pty.read().await;
        if let Some(h) = pty.as_ref() {
            let _ = nix::sys::signal::kill(h.child_pid, nix::sys::signal::Signal::SIGTERM);
        }
        drop(pty);
        *self.pty.write().await = None;
        *self.state.write().await = SlotState::Unborn;
        tracing::info!(slot = %self.config.name, "slot killed → unborn");
    }

    /// Restart a dead slot.
    pub async fn restart(&self) -> Result<()> {
        {
            let mut pty = self.pty.write().await;
            *pty = None;
        }
        *self.state.write().await = SlotState::Unborn;
        if !self.config.lazy {
            self.boot().await?;
        }
        Ok(())
    }

    pub async fn current_state(&self) -> SlotState {
        self.state.read().await.clone()
    }

    pub fn gen(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    pub async fn idle_duration(&self) -> Duration {
        Instant::now().duration_since(*self.last_active.read().await)
    }
}
