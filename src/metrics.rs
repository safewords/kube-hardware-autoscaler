//! Prometheus metrics and the health/metrics HTTP server.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use prometheus::{Encoder, GaugeVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};

use crate::crd::PowerState;

pub struct Metrics {
    registry: Registry,
    power_actions: IntCounterVec,
    node_power: IntGaugeVec,
    pool_nodes: IntGaugeVec,
    pool_pending: IntGaugeVec,
    reconcile_errors: IntCounterVec,
    probe_up: IntGaugeVec,
    probe_rtt: GaugeVec,
    boot_average: GaugeVec,
    /// Set once the informer caches are synced.
    pub ready: AtomicBool,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();
        let power_actions = IntCounterVec::new(
            Opts::new(
                "kha_power_actions_total",
                "Power actions issued to management interfaces",
            ),
            &["node", "action", "result"],
        )
        .unwrap();
        let node_power = IntGaugeVec::new(
            Opts::new("kha_node_power_state", "Observed power state (1 on, 0 off, -1 unknown)"),
            &["node"],
        )
        .unwrap();
        let pool_nodes = IntGaugeVec::new(
            Opts::new("kha_pool_nodes", "Machines per pool by state"),
            &["pool", "state"],
        )
        .unwrap();
        let pool_pending = IntGaugeVec::new(
            Opts::new("kha_pool_pending_pods", "Unschedulable pods the pool could serve"),
            &["pool"],
        )
        .unwrap();
        let reconcile_errors = IntCounterVec::new(
            Opts::new("kha_reconcile_errors_total", "Failed reconciliations"),
            &["controller"],
        )
        .unwrap();
        let probe_up = IntGaugeVec::new(
            Opts::new(
                "kha_machine_probe_up",
                "Whether the machine's management path answered its last probe (spec.probe)",
            ),
            &["node"],
        )
        .unwrap();
        let probe_rtt = GaugeVec::new(
            Opts::new(
                "kha_machine_probe_rtt_seconds",
                "Round-trip time of the machine's last answered management probe",
            ),
            &["node"],
        )
        .unwrap();
        let boot_average = GaugeVec::new(
            Opts::new(
                "kha_machine_boot_average_seconds",
                "Mean time from power-on to Node Ready over the machine's recent boots",
            ),
            &["node"],
        )
        .unwrap();
        for c in [
            Box::new(power_actions.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(node_power.clone()),
            Box::new(pool_nodes.clone()),
            Box::new(pool_pending.clone()),
            Box::new(reconcile_errors.clone()),
            Box::new(probe_up.clone()),
            Box::new(probe_rtt.clone()),
            Box::new(boot_average.clone()),
        ] {
            registry.register(c).unwrap();
        }
        Arc::new(Self {
            registry,
            power_actions,
            node_power,
            pool_nodes,
            pool_pending,
            reconcile_errors,
            probe_up,
            probe_rtt,
            boot_average,
            ready: AtomicBool::new(false),
        })
    }

    pub fn power_action(&self, node: &str, action: &str, ok: bool) {
        self.power_actions
            .with_label_values(&[node, action, if ok { "success" } else { "error" }])
            .inc();
    }

    pub fn node_power(&self, node: &str, state: PowerState) {
        let v = match state {
            PowerState::On => 1,
            PowerState::Off => 0,
            PowerState::Unknown => -1,
        };
        self.node_power.with_label_values(&[node]).set(v);
    }

    pub fn pool(&self, pool: &str, online: u32, total: u32, pending: u32) {
        self.pool_nodes.with_label_values(&[pool, "online"]).set(online as i64);
        self.pool_nodes.with_label_values(&[pool, "total"]).set(total as i64);
        self.pool_pending.with_label_values(&[pool]).set(pending as i64);
    }

    /// Records a machine's reachability; `None` when it is not probed.
    pub fn probe(&self, node: &str, reachability: Option<&crate::crd::Reachability>) {
        match reachability {
            Some(r) => {
                self.probe_up.with_label_values(&[node]).set(r.reachable as i64);
                match r.rtt_ms {
                    Some(ms) => self.probe_rtt.with_label_values(&[node]).set(ms as f64 / 1000.0),
                    None => {
                        let _ = self.probe_rtt.remove_label_values(&[node]);
                    }
                }
            }
            None => {
                let _ = self.probe_up.remove_label_values(&[node]);
                let _ = self.probe_rtt.remove_label_values(&[node]);
            }
        }
    }

    pub fn boot_average(&self, node: &str, seconds: Option<u64>) {
        match seconds {
            Some(s) => self.boot_average.with_label_values(&[node]).set(s as f64),
            None => {
                let _ = self.boot_average.remove_label_values(&[node]);
            }
        }
    }

    pub fn reconcile_error(&self, controller: &str) {
        self.reconcile_errors.with_label_values(&[controller]).inc();
    }

    fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf).ok();
        String::from_utf8(buf).unwrap_or_default()
    }
}

async fn metrics(State(m): State<Arc<Metrics>>) -> String {
    m.render()
}

async fn readyz(State(m): State<Arc<Metrics>>) -> StatusCode {
    if m.ready.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Serves `/metrics`, `/healthz` and `/readyz`.
pub async fn serve(addr: SocketAddr, metrics_: Arc<Metrics>) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/metrics", get(metrics))
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .with_state(metrics_);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "serving metrics and health endpoints");
    axum::serve(listener, app).await?;
    Ok(())
}
