# meshd

A Rust daemon that runs AI coding agents in named terminal slots and puts an HTTP API in front of them. Each slot is a long-lived PTY process. You can send it a prompt, read its recent output, stream it live and check its state. Several machines can run meshd and relay to each other.

I built it to run coding agents (Claude Code, Codex, Gemini, OpenCode) on a few Macs over Tailscale and coordinate them from one place.

---

## Why not just tmux?

`tmux send-keys` works on one machine. It gets hard to trust beyond that:

- **Known state instead of PID guessing.** Each slot has a defined state: `Unborn`, `Spawning`, `Ready` or `Busy`. You always know whether a slot exists and what it's doing.
- **HTTP instead of send-keys.** send-keys is fragile with special characters, multi-line prompts and concurrent writers. A POST to `/inject/{slot}` is one authenticated write and returns a generation counter.
- **Ring buffers for output.** `capture-pane` returns screen content, truncated and lossy. meshd keeps a 500-line ring buffer per slot and streams it over SSE.
- **Optional heartbeat.** Each machine can push its slot states to a Supabase `agent_slots` table every 60 seconds, so one query shows every agent on every machine.
- **Auth and rate limits.** Bearer token on every write endpoint, an IP allowlist (Tailscale range and localhost) on mesh endpoints, and a 10-second rate limit per endpoint key.

It also replaces an older Python mesh API: `/relay`, `/broadcast`, `/inject-tmux`, `/task` and `/panes` are endpoints on the same binary.

---

## Quick start

**Build:**
```bash
cargo build --release
# binary: target/release/meshd
```

**Config** (`~/.meshd/meshd.toml`):
```toml
[machine]
id = "studio"
tailscale_ip = "100.64.0.1"
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
MESHD_TOKEN=your_token MESHD_NODES=studio=100.64.0.1,render=100.64.0.2 \
  ./target/release/meshd ~/.meshd/meshd.toml
```

---

## API

All endpoints are on `:9451`. Write endpoints need `Authorization: Bearer <token>`. `/health` has no auth.

### Slots

**Send a prompt to a slot:**
```bash
curl -X POST http://localhost:9451/inject/claude-max \
  -H "Authorization: Bearer your_token" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "summarize the last 3 commits in ~/src/myproject"}'
# {"ok":true,"gen":4,"slot":"claude-max"}
```

**Stream live output (SSE):**
```bash
curl -N http://localhost:9451/stream/claude-max \
  -H "Authorization: Bearer your_token"
# Replays the last 50 lines from the ring buffer, then streams live output
# event: line
# data: > Running task...
```

**Read the ring buffer:**
```bash
curl "http://localhost:9451/read/claude-max?n=20" \
  -H "Authorization: Bearer your_token"
# {"slot":"claude-max","lines":["line1","line2",...]}
```

**All slot states:**
```bash
curl http://localhost:9451/status \
  -H "Authorization: Bearer your_token"
# {"machine":"studio","slots":[{"name":"claude-max","state":"ready","gen":4,"pid":12345},...]}
```

**Health (no auth):**
```bash
curl http://localhost:9451/health
# {"machine":"studio","uptime_secs":3821,"slots":5}
```

### Mesh

These need auth and a Tailscale or localhost client IP. The 10-second rate limit applies per endpoint key.

**Relay a request to another machine:**
```bash
curl -X POST http://localhost:9451/relay \
  -H "Authorization: Bearer your_token" \
  -d '{"machine":"render","path":"/inject/codex","body":{"prompt":"run tests"}}'
```

**Broadcast to every other node:**
```bash
curl -X POST http://localhost:9451/broadcast \
  -H "Authorization: Bearer your_token" \
  -d '{"prompt":"checkpoint your current work"}'
# Fans out to every node in MESHD_NODES except this one. Returns per-node results.
```

**Send text to a tmux session:**
```bash
# Local
curl -X POST http://localhost:9451/inject-tmux \
  -H "Authorization: Bearer your_token" \
  -d '{"prompt":"git status","session":"main"}'

# Remote (SCP + SSH)
curl -X POST http://localhost:9451/inject-tmux \
  -H "Authorization: Bearer your_token" \
  -d '{"machine":"render","prompt":"git status","session":"main"}'
```

**Queue and claim tasks:**
```bash
# Queue
curl -X POST http://localhost:9451/task \
  -H "Authorization: Bearer your_token" \
  -d '{"prompt":"run the nightly sync","from":"studio"}'

# List pending
curl http://localhost:9451/tasks -H "Authorization: Bearer your_token"

# Claim the oldest unclaimed task (lock files prevent a double claim)
curl "http://localhost:9451/tasks/claim?claimer=render" \
  -H "Authorization: Bearer your_token"
```

**Mesh health:**
```bash
curl http://localhost:9451/mesh -H "Authorization: Bearer your_token"
# Pings /health on every node in parallel, 3 s timeout each
```

**Local agent panes:**
```bash
curl http://localhost:9451/panes -H "Authorization: Bearer your_token"
# Lists agent processes with their TTYs and PIDs
```

**Read a tmux pane:**
```bash
curl "http://localhost:9451/read-pane/ttys003?lines=30" \
  -H "Authorization: Bearer your_token"
```

---

## Configuration

```toml
[machine]
id = "studio"                        # This node's name, as used in MESHD_NODES
tailscale_ip = "100.64.0.1"
api_port = 9451                      # Main HTTP port
mesh_port = 9450                     # Optional second listener for older clients
supabase_url = "https://..."         # Heartbeat registry
mesh_auth_token = "override-token"   # Optional, overrides MESHD_TOKEN

[[slots]]
name = "my-agent"         # URL-safe, used in /inject/{name}
agent = "/usr/bin/myagent"
args = ["--flag"]
env = { MY_KEY = "value" }
lazy = true               # false = boot at startup, true = boot on first inject
idle_timeout_secs = 1800  # Lazy slots only: stop after this many idle seconds (default 1800)
rows = 50                 # PTY height (default 50)
cols = 220                # PTY width (default 220)
```

**Environment:**
- `MESHD_TOKEN`: bearer token for authenticated endpoints
- `MESHD_NODES`: the mesh, as comma-separated `name=ip` pairs. Relay, broadcast and `/mesh` use it. Without it meshd uses two example nodes.
- `SUPABASE_ANON_KEY`: turns on registration and the heartbeat. meshd starts without it and logs a warning.
- `RUST_LOG`: log level, e.g. `meshd=debug`

---

## Architecture

```
src/
  main.rs       startup, config load, eager slot boot, registration
  config.rs     TOML config (MachineConfig, SlotConfig)
  http.rs       axum router and handlers
  slot.rs       slot state machine, inject / read / subscribe
  pty.rs        fork + execve, PTY setup, ring buffer and broadcast channel
  watchdog.rs   10 s poll: restart dead slots, stop idle lazy slots, warn on stuck Busy
  registry.rs   Supabase upsert on boot, heartbeat every 60 s
  mesh.rs       nodes, relay, broadcast, tmux injection, task queue, IP allowlist
```

### Slot lifecycle

```
Unborn -> Spawning -> Ready -> Busy
                       ^        |
                       |        v
                       +------ (watchdog restarts dead processes)
```

- Eager slots (`lazy = false`) boot when meshd starts.
- Lazy slots boot on the first `/inject` or `/stream`.
- The watchdog runs every 10 seconds. It restarts a slot whose process exited and stops lazy slots idle longer than `idle_timeout_secs`.
- A slot stuck in `Busy` with no output for 10 minutes gets a logged warning, not a kill.

### Output

A tokio task per slot reads the PTY, strips ANSI escapes, splits lines and writes each line to:
1. a 500-line ring buffer
2. a broadcast channel (256 deep) for SSE subscribers

`/stream` replays the last 50 lines from the ring, then follows the live channel.

### Task queue

Tasks are JSON files in `~/.claude/state/mesh-inbox/`. A claim creates a `.lock` file with `O_CREAT | O_EXCL`, which is atomic on POSIX, so two machines can't claim the same task.

### IP allowlist

Mesh endpoints (`/relay`, `/broadcast`, `/inject-tmux`, `/message`, `/task`, `/mesh`) check the client IP. Allowed: `127.0.0.1`, `::1` and the Tailscale range `100.64.0.0/10`. Anything else gets a 403.

---

## Dependencies

| Crate | Purpose |
|-------|---------|
| tokio | async runtime |
| axum 0.7 | HTTP server |
| nix 0.29 | PTY (openpty, fork, execve) |
| reqwest 0.12 | relay and heartbeat |
| tokio-stream | SSE from the broadcast channel |
| serde, toml | config and JSON |
| tracing | logging |
| chrono | timestamps |

---

## Notes

- Child processes don't inherit `CLAUDECODE`, so agents don't think they're running inside Claude Code.
- PTYs default to 50 × 220. Wide columns mean less line wrapping in the ring buffer.
- Max prompt size for tmux injection is 10 KB.
