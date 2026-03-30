use anyhow::Result;
use nix::libc;
use nix::pty::{openpty, Winsize};
use nix::unistd::{dup2, execve, fork, ttyname, ForkResult, Pid};
use std::collections::VecDeque;
use std::ffi::CString;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, RawFd};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;
use tokio::sync::broadcast;

const RING_CAPACITY: usize = 500;

#[derive(Clone)]
pub struct PtyHandle {
    pub master_fd: RawFd,
    pub slave_path: PathBuf,
    pub child_pid: Pid,
    pub tx: broadcast::Sender<String>,
    pub ring: Arc<Mutex<VecDeque<String>>>,
}

pub struct SpawnConfig<'a> {
    pub agent: &'a str,
    pub args: &'a [String],
    pub env: &'a std::collections::HashMap<String, String>,
    pub rows: u16,
    pub cols: u16,
}

pub fn spawn_slot(cfg: SpawnConfig<'_>) -> Result<PtyHandle> {
    let ws = Winsize {
        ws_row: cfg.rows,
        ws_col: cfg.cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };

    let pty_result = openpty(Some(&ws), None)?;
    let slave_path = ttyname(pty_result.slave.as_fd())?;

    // Extract raw fds before fork
    let master_raw = pty_result.master.as_raw_fd();
    let slave_raw = pty_result.slave.as_raw_fd();

    let agent_cstr = CString::new(cfg.agent)?;
    let mut arg_cstrs: Vec<CString> = vec![CString::new(cfg.agent)?];
    for a in cfg.args {
        arg_cstrs.push(CString::new(a.as_str())?);
    }

    // Build env: inherit current env + overlay slot-specific vars, strip CLAUDECODE
    let mut env_map: std::collections::HashMap<String, String> = std::env::vars().collect();
    for (k, v) in cfg.env {
        env_map.insert(k.clone(), v.clone());
    }
    env_map.remove("CLAUDECODE");
    let env_cstrs: Vec<CString> = env_map
        .iter()
        .filter_map(|(k, v)| CString::new(format!("{}={}", k, v)).ok())
        .collect();

    // Consume OwnedFds before fork so they live in the right scope
    let master_owned = pty_result.master;
    let slave_owned = pty_result.slave;

    let child_pid = unsafe {
        match fork()? {
            ForkResult::Parent { child, .. } => {
                // Parent: close slave, keep master
                drop(slave_owned);
                // Keep master_owned alive in parent scope
                std::mem::forget(master_owned);
                child
            }
            ForkResult::Child => {
                // Child: new session
                if libc::setsid() < 0 {
                    std::process::exit(1);
                }
                // Acquire controlling terminal
                if libc::ioctl(slave_raw, libc::TIOCSCTTY as _, 0i32) < 0 {
                    std::process::exit(1);
                }
                // Wire stdio
                dup2(slave_raw, libc::STDIN_FILENO).unwrap_or_else(|_| std::process::exit(1));
                dup2(slave_raw, libc::STDOUT_FILENO).unwrap_or_else(|_| std::process::exit(1));
                dup2(slave_raw, libc::STDERR_FILENO).unwrap_or_else(|_| std::process::exit(1));

                if slave_raw > 2 {
                    let _ = libc::close(slave_raw);
                }
                let _ = libc::close(master_raw);

                execve(&agent_cstr, &arg_cstrs, &env_cstrs)
                    .unwrap_or_else(|_| std::process::exit(127));
                unreachable!()
            }
        }
    };

    let (tx, _) = broadcast::channel::<String>(256);
    let ring: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::with_capacity(RING_CAPACITY)));

    // Background reader: master_raw → ring + broadcast
    let tx_clone = tx.clone();
    let ring_clone = ring.clone();
    // dup master so we can give it to the async task
    let master_dup = unsafe { libc::dup(master_raw) };
    if master_dup < 0 {
        return Err(anyhow::anyhow!("dup(master_fd) failed"));
    }

    tokio::spawn(async move {
        let std_file = unsafe { std::fs::File::from_raw_fd(master_dup) };
        let mut file = tokio::fs::File::from_std(std_file);
        let mut buf = [0u8; 4096];
        let mut partial = String::new();

        loop {
            match file.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    // Strip ANSI sequences and split on newlines
                    let raw = String::from_utf8_lossy(&buf[..n]).to_string();
                    let stripped = strip_ansi(&raw);
                    partial.push_str(&stripped);

                    while let Some(pos) = partial.find('\n') {
                        let line = partial[..pos].trim_end_matches('\r').trim().to_string();
                        partial = partial[pos + 1..].to_string();
                        if line.is_empty() {
                            continue;
                        }
                        {
                            let mut r = ring_clone.lock().unwrap();
                            if r.len() >= RING_CAPACITY {
                                r.pop_front();
                            }
                            r.push_back(line.clone());
                        }
                        let _ = tx_clone.send(line);
                    }
                }
            }
        }
    });

    Ok(PtyHandle {
        master_fd: master_raw,
        slave_path,
        child_pid,
        tx,
        ring,
    })
}

pub fn write_to_pty(master_fd: RawFd, data: &[u8]) -> Result<()> {
    // Use libc write — borrowing the fd without taking ownership
    let res = unsafe {
        libc::write(
            master_fd,
            data.as_ptr() as *const libc::c_void,
            data.len(),
        )
    };
    if res < 0 {
        return Err(anyhow::anyhow!("write_to_pty failed: errno {}", std::io::Error::last_os_error()));
    }
    Ok(())
}

/// Minimal ANSI escape sequence stripper.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            match chars.peek() {
                Some('[') => {
                    chars.next(); // consume '['
                    // consume until final byte (letter or certain punctuation)
                    for nc in chars.by_ref() {
                        if nc.is_ascii_alphabetic() || nc == 'm' || nc == 'J' || nc == 'K' || nc == 'H' {
                            break;
                        }
                    }
                }
                Some(']') => {
                    chars.next();
                    // OSC — consume until BEL or ST
                    for nc in chars.by_ref() {
                        if nc == '\x07' || nc == '\u{009c}' {
                            break;
                        }
                    }
                }
                _ => {} // lone ESC — skip
            }
        } else {
            out.push(c);
        }
    }
    out
}
