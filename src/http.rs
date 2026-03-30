use crate::mesh::{self, RateLimiter};
use crate::slot::{Slot, SlotState};
use axum::{
    body::Body,
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

pub type SlotMap = Arc<HashMap<String, Arc<Slot>>>;

#[derive(Clone)]
pub struct AppState {
    pub slots: SlotMap,
    pub machine_id: String,
    pub token: String,
    pub rate_limiter: Arc<RateLimiter>,
    pub api_port: u16,
}

#[derive(Deserialize)]
pub struct InjectBody {
    pub prompt: String,
}

#[derive(Serialize)]
pub struct InjectResponse {
    pub ok: bool,
    pub gen: u64,
    pub slot: String,
}

#[derive(Serialize)]
pub struct SlotStatus {
    pub name: String,
    pub state: String,
    pub gen: u64,
    pub pid: Option<u32>,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub machine: String,
    pub slots: Vec<SlotStatus>,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub machine: String,
    pub uptime_secs: u64,
    pub slots: usize,
}

#[derive(Deserialize)]
pub struct ReadQuery {
    pub n: Option<usize>,
}

static START_TIME: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

pub fn router(state: AppState) -> Router {
    START_TIME.get_or_init(std::time::Instant::now);
    Router::new()
        // Existing slot endpoints
        .route("/inject/:slot", post(inject_handler))
        .route("/stream/:slot", get(stream_handler))
        .route("/read/:slot", get(read_handler))
        .route("/status", get(status_handler))
        .route("/health", get(health_handler))
        // Mesh-API endpoints (merged from Python daemon)
        .route("/inject-tmux", post(inject_tmux_handler))
        .route("/relay", post(relay_handler))
        .route("/broadcast", post(broadcast_handler))
        .route("/message", post(message_handler))
        .route("/task", post(task_handler))
        .route("/panes", get(panes_handler))
        .route("/read-pane/:tty", get(read_pane_handler))
        .route("/tasks", get(tasks_handler))
        .route("/tasks/claim", get(claim_task_handler))
        .route("/mesh", get(mesh_handler))
        .with_state(state)
}

fn check_auth(headers: &HeaderMap, token: &str) -> bool {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == format!("Bearer {}", token))
        .unwrap_or(false)
}

async fn inject_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slot_name): Path<String>,
    Json(body): Json<InjectBody>,
) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "unauthorized"}))).into_response();
    }

    let slot = match state.slots.get(&slot_name) {
        Some(s) => s,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": format!("slot '{}' not found", slot_name)})),
            ).into_response()
        }
    };

    match slot.inject(&body.prompt).await {
        Ok(gen) => Json(InjectResponse { ok: true, gen, slot: slot_name }).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ).into_response(),
    }
}

async fn stream_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slot_name): Path<String>,
) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }

    let slot = match state.slots.get(&slot_name) {
        Some(s) => s.clone(),
        None => return (StatusCode::NOT_FOUND, "slot not found").into_response(),
    };

    // Boot lazily if needed so subscriber has a channel to attach to
    if slot.current_state().await == SlotState::Unborn {
        if let Err(e) = slot.boot().await {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    }

    let rx = match slot.subscribe().await {
        Some(r) => r,
        None => return (StatusCode::SERVICE_UNAVAILABLE, "slot not ready").into_response(),
    };

    // Drain ring first, then live
    let backlog = slot.read_ring(50).await;

    let stream = async_stream::stream! {
        // Emit backlog
        for line in &backlog {
            let event = format!("event: line\ndata: {}\n\n", line);
            yield Ok::<_, std::convert::Infallible>(bytes::Bytes::from(event));
        }
        // Live stream
        let mut broadcast_stream = BroadcastStream::new(rx);
        while let Some(item) = broadcast_stream.next().await {
            match item {
                Ok(line) => {
                    let event = format!("event: line\ndata: {}\n\n", line);
                    yield Ok(bytes::Bytes::from(event));
                }
                Err(_) => break, // Lagged — client should reconnect
            }
        }
    };

    Response::builder()
        .status(200)
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("X-Accel-Buffering", "no")
        .body(Body::from_stream(stream))
        .unwrap()
        .into_response()
}

async fn read_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slot_name): Path<String>,
    Query(query): Query<ReadQuery>,
) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "unauthorized"}))).into_response();
    }

    let slot = match state.slots.get(&slot_name) {
        Some(s) => s,
        None => {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "slot not found"}))).into_response()
        }
    };

    let n = query.n.unwrap_or(50);
    let lines = slot.read_ring(n).await;
    Json(serde_json::json!({ "slot": slot_name, "lines": lines })).into_response()
}

async fn status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "unauthorized"}))).into_response();
    }

    let mut slot_statuses = vec![];
    for (name, slot) in state.slots.iter() {
        let st = slot.current_state().await;
        let pid = {
            let pty = slot.pty.read().await;
            pty.as_ref().map(|h| h.child_pid.as_raw() as u32)
        };
        slot_statuses.push(SlotStatus {
            name: name.clone(),
            state: st.to_string(),
            gen: slot.gen(),
            pid,
        });
    }
    slot_statuses.sort_by(|a, b| a.name.cmp(&b.name));

    Json(StatusResponse {
        machine: state.machine_id,
        slots: slot_statuses,
    }).into_response()
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let uptime = START_TIME
        .get()
        .map(|t| t.elapsed().as_secs())
        .unwrap_or(0);
    Json(HealthResponse {
        machine: state.machine_id,
        uptime_secs: uptime,
        slots: state.slots.len(),
    })
}

// ── Mesh-API Handlers (merged from Python daemon) ──────────────────────

// Helper: extract client IP from ConnectInfo or headers
fn extract_client_ip(connect_info: &ConnectInfo<SocketAddr>, headers: &HeaderMap) -> String {
    // Check X-Forwarded-For first
    if let Some(xff) = headers.get("x-forwarded-for") {
        if let Ok(s) = xff.to_str() {
            if let Some(first) = s.split(',').next() {
                return first.trim().to_string();
            }
        }
    }
    connect_info.0.ip().to_string()
}

fn check_ip_and_auth(
    headers: &HeaderMap,
    token: &str,
    connect_info: &ConnectInfo<SocketAddr>,
) -> Option<(StatusCode, Json<serde_json::Value>)> {
    if !check_auth(headers, token) {
        return Some((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        ));
    }
    let ip = extract_client_ip(connect_info, headers);
    if !mesh::is_ip_allowed(&ip) {
        return Some((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": format!("IP {} not allowed", ip)})),
        ));
    }
    None
}

async fn check_rate_limit(
    rate_limiter: &RateLimiter,
    key: &str,
) -> Option<(StatusCode, Json<serde_json::Value>)> {
    if let Err(remaining) = rate_limiter.check(key).await {
        return Some((
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({
                "error": "rate limited",
                "retry_after_secs": remaining
            })),
        ));
    }
    None
}

// ── /inject-tmux ───────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct InjectTmuxBody {
    pub machine: Option<String>,
    pub prompt: String,
    pub session: Option<String>,
}

async fn inject_tmux_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(body): Json<InjectTmuxBody>,
) -> impl IntoResponse {
    let ci = ConnectInfo(addr);
    if let Some(err) = check_ip_and_auth(&headers, &state.token, &ci) {
        return err.into_response();
    }
    if let Some(err) = check_rate_limit(&state.rate_limiter, "inject-tmux").await {
        return err.into_response();
    }

    let machine = body.machine.as_deref().unwrap_or("local");
    let session = body.session.as_deref().unwrap_or("main");

    match mesh::inject_tmux(machine, &body.prompt, session).await {
        Ok(msg) => Json(serde_json::json!({"ok": true, "message": msg})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ── /relay ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct RelayBody {
    pub machine: String,
    pub path: String,
    pub body: Option<serde_json::Value>,
}

async fn relay_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(body): Json<RelayBody>,
) -> impl IntoResponse {
    let ci = ConnectInfo(addr);
    if let Some(err) = check_ip_and_auth(&headers, &state.token, &ci) {
        return err.into_response();
    }
    if let Some(err) = check_rate_limit(&state.rate_limiter, &format!("relay-{}", body.machine)).await {
        return err.into_response();
    }

    let relay_body = body.body.unwrap_or(serde_json::json!({}));
    match mesh::relay(&body.machine, &body.path, relay_body, &state.token).await {
        Ok(resp) => Json(serde_json::json!({"ok": true, "response": resp})).into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ── /broadcast ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct BroadcastBody {
    pub prompt: String,
    pub exclude: Option<String>,
}

async fn broadcast_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(body): Json<BroadcastBody>,
) -> impl IntoResponse {
    let ci = ConnectInfo(addr);
    if let Some(err) = check_ip_and_auth(&headers, &state.token, &ci) {
        return err.into_response();
    }
    if let Some(err) = check_rate_limit(&state.rate_limiter, "broadcast").await {
        return err.into_response();
    }

    let exclude = body.exclude.as_deref().unwrap_or(&state.machine_id);
    let results = mesh::broadcast(&body.prompt, exclude, &state.token).await;
    Json(serde_json::json!({"ok": true, "results": results})).into_response()
}

// ── /message ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct MessageBody {
    pub prompt: String,
    pub session: Option<String>,
}

async fn message_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(body): Json<MessageBody>,
) -> impl IntoResponse {
    let ci = ConnectInfo(addr);
    if let Some(err) = check_ip_and_auth(&headers, &state.token, &ci) {
        return err.into_response();
    }
    if let Some(err) = check_rate_limit(&state.rate_limiter, "message").await {
        return err.into_response();
    }

    let session = body.session.as_deref().unwrap_or("main");
    match mesh::inject_tmux("local", &body.prompt, session).await {
        Ok(msg) => Json(serde_json::json!({"ok": true, "message": msg})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ── /task ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct TaskBody {
    pub prompt: String,
    pub from: Option<String>,
}

async fn task_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(body): Json<TaskBody>,
) -> impl IntoResponse {
    let ci = ConnectInfo(addr);
    if let Some(err) = check_ip_and_auth(&headers, &state.token, &ci) {
        return err.into_response();
    }
    if let Some(err) = check_rate_limit(&state.rate_limiter, "task").await {
        return err.into_response();
    }

    let from = body.from.as_deref().unwrap_or("unknown");
    match mesh::queue_task(&body.prompt, from).await {
        Ok(task) => Json(serde_json::json!({"ok": true, "task": task})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ── /panes ─────────────────────────────────────────────────────────────

async fn panes_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        )
            .into_response();
    }

    match mesh::get_local_panes().await {
        Ok(panes) => Json(serde_json::json!({"panes": panes})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ── /read-pane/:tty ────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ReadPaneQuery {
    pub lines: Option<usize>,
}

async fn read_pane_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(tty): Path<String>,
    Query(query): Query<ReadPaneQuery>,
) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        )
            .into_response();
    }

    let lines = query.lines.unwrap_or(50);
    match mesh::read_pane(&tty, lines).await {
        Ok(output) => Json(serde_json::json!({"tty": tty, "output": output})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ── /tasks ─────────────────────────────────────────────────────────────

async fn tasks_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        )
            .into_response();
    }

    match mesh::list_tasks().await {
        Ok(tasks) => Json(serde_json::json!({"tasks": tasks})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ── /tasks/claim ───────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ClaimQuery {
    pub claimer: Option<String>,
}

async fn claim_task_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ClaimQuery>,
) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        )
            .into_response();
    }

    let claimer = query.claimer.as_deref().unwrap_or(&state.machine_id);
    match mesh::claim_task(claimer).await {
        Ok(Some(task)) => Json(serde_json::json!({"ok": true, "task": task})).into_response(),
        Ok(None) => Json(serde_json::json!({"ok": true, "task": null})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ── /mesh ──────────────────────────────────────────────────────────────

async fn mesh_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !check_auth(&headers, &state.token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        )
            .into_response();
    }

    let nodes = mesh::discover_mesh(state.api_port).await;
    Json(serde_json::json!({
        "machine": state.machine_id,
        "nodes": nodes
    }))
    .into_response()
}
