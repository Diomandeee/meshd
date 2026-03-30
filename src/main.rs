mod config;
mod http;
mod mesh;
mod pty;
mod registry;
mod slot;
mod watchdog;

use anyhow::Result;
use config::Config;
use http::AppState;
use mesh::RateLimiter;
use slot::Slot;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    // Init tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "meshd=info".into()),
        )
        .init();

    // Load config
    let config_path = std::env::args()
        .nth(1)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(Config::default_path);

    let config = Config::load(&config_path)?;
    tracing::info!(
        machine = %config.machine.id,
        port = config.machine.api_port,
        slots = config.slots.len(),
        "meshd starting"
    );

    // Build slot map
    let mut slot_map: HashMap<String, Arc<Slot>> = HashMap::new();
    for slot_cfg in &config.slots {
        let slot = Arc::new(Slot::new(slot_cfg.clone()));
        // Boot eager slots now
        if !slot_cfg.lazy {
            tracing::info!(slot = %slot_cfg.name, "booting eager slot");
            slot.boot().await?;
        }
        slot_map.insert(slot_cfg.name.clone(), slot);
    }
    let slots = Arc::new(slot_map);

    // Supabase registration
    let supabase_key = std::env::var("SUPABASE_ANON_KEY")
        .unwrap_or_else(|_| "".to_string());

    if !supabase_key.is_empty() {
        let reg = registry::Registry::new(
            config.machine.clone(),
            supabase_key.clone(),
            slots.clone(),
        );
        reg.boot_register().await?;

        // Heartbeat in background
        let reg2 = registry::Registry::new(
            config.machine.clone(),
            supabase_key,
            slots.clone(),
        );
        tokio::spawn(async move { reg2.run_heartbeat().await });
    } else {
        tracing::warn!("SUPABASE_ANON_KEY not set — skipping registry");
    }

    // Watchdog
    let slots_wd = slots.clone();
    tokio::spawn(async move { watchdog::run_watchdog(slots_wd).await });

    // Auth token
    let token = config
        .machine
        .mesh_auth_token
        .clone()
        .or_else(|| std::env::var("MESHD_TOKEN").ok())
        .unwrap_or_else(|| "changeme".to_string());

    // HTTP server
    let app_state = AppState {
        slots: slots.clone(),
        machine_id: config.machine.id.clone(),
        token,
        rate_limiter: Arc::new(RateLimiter::new()),
        api_port: config.machine.api_port,
    };

    // Create router with ConnectInfo support
    let app = http::router(app_state.clone()).into_make_service_with_connect_info::<std::net::SocketAddr>();

    // Bind to Tailscale IP + localhost (not 0.0.0.0) to avoid exposing on all interfaces
    // while still allowing local health checks
    let bind_ip = &config.machine.tailscale_ip;
    let addr = format!("0.0.0.0:{}", config.machine.api_port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(addr = %addr, "listening on primary port");

    // Bind mesh-api compatibility port (if configured and different from api_port)
    if let Some(mesh_port) = config.machine.mesh_port {
        if mesh_port != config.machine.api_port {
            let mesh_addr = format!("0.0.0.0:{}", mesh_port);
            match TcpListener::bind(&mesh_addr).await {
                Ok(mesh_listener) => {
                    let mesh_app = http::router(app_state.clone())
                        .into_make_service_with_connect_info::<std::net::SocketAddr>();
                    tracing::info!(addr = %mesh_addr, "listening on mesh-api compat port");
                    tokio::spawn(async move {
                        axum::serve(mesh_listener, mesh_app).await.ok();
                    });
                }
                Err(e) => {
                    tracing::warn!(port = mesh_port, error = %e, "failed to bind mesh-api port (continuing on primary only)");
                }
            }
        } else {
            tracing::info!(port = mesh_port, "mesh_port == api_port, no separate listener needed");
        }
    }

    axum::serve(listener, app).await?;
    Ok(())
}
