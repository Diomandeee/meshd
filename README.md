# meshd

A Rust daemon that manages AI agent terminals (PTYs) across a fleet of machines. Each agent runs in a named slot — a long-lived PTY process with an HTTP interface for injection, streaming output, and status monitoring.

Runs on all 5 machines in a Tailscale mesh. Coordinates 61 agents across 4 AI providers (Claude, OpenCode, Gemini, Codex).

---

## Why not just tmux?

tmux send-keys works fine for a single machine. It breaks down as soon as you want more:

- **Static registry vs dynamic discovery.** With tmux, you grep for PIDs and hope they match. meshd has a named slot registry with defined states (`Unborn`, `Spawning`, `Ready`, `Busy`). You always know whether a slot exists.
- **HTTP API vs send-keys.** `tmux send-keys` is fragile with special characters, multi-line prompts, and concurrent access. A POST to `/inject/{slot}` is atomic, authenticated, and returns a generation counter.
- **Ring buffers for output capture.** tmux capture-pane gives you screen content — static, truncated, and lossy. meshd maintains a 500-line ring buffer per slot, drained over SSE. You can read the last N lines or subscribe to the live stream.
- **Supabase heartbeat for fleet visibility.** Each machine pushes slot state to a central `agent_slots` table every 60 seconds. You get a live view of all 61 agents across 5 machines in a single query.
- **Rate limiting and auth.** Bearer token on every write endpoint. IP allowlisting for mesh endpoints (Tailscale range + localhost only). 10-second rate limit per endpoint key.

meshd also absorbs the old Python mesh-api — `/relay`, `/broadcast`, `/inject-tmux`, `/task`, `/panes` are all first-class endpoints on the same binary.

---

## Quick Start

**Build:**
```bash
cargo build --release
# binary: target/release/meshd (4MB, stripped)
# compile time: ~30s on M-series
```

**Config** (`~/.meshd/meshd.toml`):
```toml
[machine]
id = "mac1"
tailscale_ip = "100.84.118.106"
api_port = 9451
supabase_url = "https://your-project.supabase.co"

[[slots]]
name = "claude-max"
agent = "/usr/local/bin/claude"
args = ["--model", "max"]
env = { HOME = "/Users/you" }
lazy = true
idle_timeout_secs = 3600

[[slots]]
name = "codex"
agent = "/usr/local/bin/codex"
args = ["--full-auto"]
lazy = false   # eager: boots at startup
```

**Run:**
```bash
SUPABASE_ANON_KEY=your_key MESHD_TOKEN=your_token ./meshd ~/.meshd/meshd.toml
```

**Install as a LaunchAgent (macOS):**
```bash
cp launchagent/com.mesh.meshd.plist ~/Library/LaunchAgents/
# Edit the plist to set SUPABASE_ANON_KEY and MESHD_TOKEN
launchctl load ~/Library/LaunchAgents/com.mesh.meshd.plist
```

---

## API Reference

All endpoints on `:9451`. Write endpoints require `Authorization: Bearer <token>`. `/health` has no auth.

### Slot endpoints

**Inject a prompt into a slot:**
```bash
curl -X POST http://localhost:9451/inject/claude-max \
  -H "Authorization: Bearer your_token" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "summarize the last 3 commits in ~/Desktop/myproject"}'
# {"ok":true,"gen":4,"slot":"claude-max"}
```

**Stream live output (SSE):**
```bash
curl -N http://localhost:9451/stream/claude-max \
  -H "Authorization: Bearer your_token"
# Replays last 50 lines from ring buffer, then streams live output
# event: line
# data: > Running task...
```

**Read ring buffer snapshot:**
```bash
curl "http://localhost:9451/read/claude-max?n=20" \
  -H "Authorization: Bearer your_token"
# {"slot":"claude-max","lines":["line1","line2",...]}
```

**All slot states:**
```bash
curl http://localhost:9451/status \
  -H "Authorization: Bearer your_token"
# {"machine":"mac1","slots":[{"name":"claude-max","state":"ready","gen":4,"pid":12345},...]}
```

**Health check (no auth):**
```bash
curl http://localhost:9451/health
# {"machine":"mac1","uptime_secs":3821,"slots":5}
```

---

### Mesh endpoints

These require auth + Tailscale/localhost IP. The 10-second rate limit applies per endpoint key.

**Relay a request to another machine:**
```bash
curl -X POST http://localhost:9451/relay \
  -H "Authorization: Bearer your_token" \
  -d '{"machine":"mac4","path":"/inject/codex","body":{"prompt":"run tests"}}'
```

**Broadcast to all nodes (except self):**
```bash
curl -X POST http://localhost:9451/broadcast \
  -H "Authorization: Bearer your_token" \
  -d '{"prompt":"checkpoint your current work"}'
# Fan-out to mac2-mac5 in parallel. Returns per-machine results.
```

**Inject into a tmux session:**
```bash
# Local
curl -X POST http://localhost:9451/inject-tmux \
  -H "Authorization: Bearer your_token" \
  -d '{"prompt":"git status","session":"main"}'

# Remote (SCP + SSH)
curl -X POST http://localhost:9451/inject-tmux \
  -H "Authorization: Bearer your_token" \
  -d '{"machine":"mac3","prompt":"git status","session":"main"}'
```

**Queue and claim tasks:**
```bash
# Queue
curl -X POST http://localhost:9451/task \
  -H "Authorization: Bearer your_token" \
  -d '{"prompt":"run the nightly sync","from":"mac1"}'

# List pending
curl http://localhost:9451/tasks -H "Authorization: Bearer your_token"

# Claim oldest unclaimed task (atomic, uses lock files to prevent double-claim)
curl "http://localhost:9451/tasks/claim?claimer=mac4" \
  -H "Authorization: Bearer your_token"
```

**Discover mesh health:**
```bash
curl http://localhost:9451/mesh -H "Authorization: Bearer your_token"
# Pings /health on all 5 nodes in parallel (3s timeout each)
```

**List local agent panes:**
```bash
curl http://localhost:9451/panes -H "Authorization: Bearer your_token"
# Lists Claude processes with their TTYs and PIDs
```

**Read a tmux pane:**
```bash
curl "http://localhost:9451/read-pane/ttys003?lines=30" \
  -H "Authorization: Bearer your_token"
```

---

## Configuration Reference

```toml
[machine]
id = "mac1"                          # Unique node identifier
tailscale_ip = "100.84.118.106"      # Used for inter-node identification
api_port = 9451                      # Primary HTTP port
mesh_port = 9450                     # Optional: compatibility port (binds a second listener)
supabase_url = "https://..."         # Project URL for registry
mesh_auth_token = "override-token"   # Optional: overrides MESHD_TOKEN env var

[[slots]]
name = "my-agent"       # URL-safe identifier, used in /inject/{name}
agent = "/usr/bin/myagent"
args = ["--flag"]
env = { MY_KEY = "value" }
lazy = true             # false = boot at startup; true = boot on first inject
idle_timeout_secs = 1800  # Lazy slots only: kill after this many idle seconds (default 1800)
rows = 50               # PTY terminal height (default 50)
cols = 220              # PTY terminal width (default 220)
```

**Environment variables:**
- `SUPABASE_ANON_KEY` — required for Supabase registration and heartbeat
- `MESHD_TOKEN` — Bearer token for all authenticated endpoints
- `RUST_LOG` — log level, e.g. `meshd=debug`

---

## Architecture

```
meshd
├── main.rs       — startup, config load, eager slot boot, Supabase registration
├── config.rs     — TOML deserialization (MachineConfig, SlotConfig)
├── http.rs       — axum router, all HTTP handlers
├── slot.rs       — Slot struct, state machine, inject/read/subscribe
├── pty.rs        — fork + execve, PTY setup, ring buffer + broadcast channel
├── watchdog.rs   — 10s poll: restart dead slots, kill idle lazy slots, warn stuck Busy
├── registry.rs   — Supabase upsert on boot, PATCH heartbeat every 60s
└── mesh.rs       — relay, broadcast, tmux injection, task queue, IP allowlist
```

### Slot lifecycle

```
Unborn → Spawning → Ready → Busy
                     ^        |
                     |        v
                     +------ (watchdog restarts dead processes)
```

- Eager slots (`lazy = false`) boot when meshd starts.
- Lazy slots boot on the first `/inject` or `/stream` call.
- The watchdog runs every 10 seconds. It restarts any slot whose child process has exited. It kills lazy slots that have been idle longer than `idle_timeout_secs`.
- A slot stuck in `Busy` with no output for 10 minutes gets a watchdog warning (logged, not killed).

### Output pipeline

Each slot has a background tokio task reading from the PTY master fd. It strips ANSI escape sequences, splits on newlines, and writes each line to:
1. A 500-line ring buffer (VecDeque, mutex-protected)
2. A broadcast channel (256 slots, for SSE subscribers)

`/stream` replays the last 50 lines from the ring, then attaches to the live broadcast channel.

### Task queue

Tasks are JSON files in `~/.claude/state/mesh-inbox/`. Claims use a `.lock` file created with `O_CREAT | O_EXCL` (atomic on POSIX) to prevent two machines from claiming the same task.

### IP allowlist

Mesh endpoints (`/relay`, `/broadcast`, `/inject-tmux`, `/message`, `/task`, `/mesh`) check the client IP. Allowed ranges: `127.0.0.1`, `::1`, Tailscale CGNAT (`100.64.0.0/10`). Requests from outside this range get a 403.

---

## Mesh Topology

Five machines connected over Tailscale:

| Node | IP | Role |
|------|----|------|
| mac1 | 100.84.118.106 | Orchestrator, build host |
| mac2 | 100.119.214.88 | iOS / motion domain |
| mac3 | 100.122.68.52 | Storage / creative domain |
| mac4 | 100.91.231.93 | Compute, Adobe pipeline |
| mac5 | 100.109.94.124 | Compute, MLX inference |

All five run the same meshd binary with machine-specific `meshd.toml`. Any node can relay to any other, or broadcast to all remaining nodes in parallel.

---

## Dependencies

| Crate | Purpose |
|-------|---------|
| tokio 1 (full) | Async runtime |
| axum 0.7 | HTTP server |
| nix 0.29 | PTY (openpty, fork, execve) |
| vt100 0.15 | Terminal emulation (imported; ANSI stripping is inline) |
| reqwest 0.12 | Outbound HTTP for relay/heartbeat |
| tokio-stream 0.1 | SSE stream from broadcast channel |
| serde + toml | Config and JSON |
| tracing | Structured logging |
| chrono | Timestamps for task queue and heartbeat |

---

## Notes

- The binary strips `CLAUDECODE` from child environments so agents don't think they're running inside Claude Code.
- PTY dimensions default to 50 rows x 220 cols. Wider columns reduce line-wrapping noise in the ring buffer.
- Supabase registration is optional. If `SUPABASE_ANON_KEY` is not set, meshd starts normally and logs a warning.
- The `mesh_port` field lets meshd bind a second listener for backward compatibility with clients still pointing at the old Python mesh-api port (9450).
- Max prompt size for tmux injection is 10KB.
