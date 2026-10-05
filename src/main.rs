use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Context as _;
use clap::{Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::runtime::events::{Recorder, Reporter};
use kube::runtime::reflector::{self, ObjectRef, Store};
use kube::runtime::{Controller, WatchStreamExt, watcher};
use kube::{Api, Client, CustomResourceExt, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use kube_hardware_autoscaler::controller::{
    Context, DecisionVerbosity, IdentityCache, node_power_management_config, node_scaling_pool,
};
use kube_hardware_autoscaler::crd::{NodePowerManagementConfig, NodeScalingPool};
use kube_hardware_autoscaler::drivers::{self, CATALOG, DriverContext, PowerChain};
use kube_hardware_autoscaler::metrics::{self, Metrics};
use kube_hardware_autoscaler::webhook;

#[derive(Parser)]
#[command(
    name = "kube-hardware-autoscaler",
    version,
    about = "Power Kubernetes nodes on and off through their management interfaces"
)]
struct Cli {
    /// Emit logs as JSON.
    #[arg(long, global = true, env = "KHA_LOG_JSON")]
    log_json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Args, Clone)]
struct DriverArgs {
    /// Namespace holding credential secrets (and Wake-on-LAN shutdown pods).
    #[arg(long, env = "POD_NAMESPACE", default_value = "kube-hardware-autoscaler")]
    namespace: String,
    /// Timeout for a single management interface operation, in seconds.
    #[arg(long, env = "KHA_OP_TIMEOUT_SECONDS", default_value_t = 60)]
    op_timeout_seconds: u64,
    /// Image for in-band shutdown, standby and sleep-probe pods (must provide
    /// sh, cat, date and nsenter). Defaults to the relay image, i.e. the
    /// operator's own Alpine image, which has them.
    #[arg(long, env = "KHA_SHUTDOWN_IMAGE")]
    shutdown_image: Option<String>,
    /// Image for Wake-on-LAN relay pods on neighbouring nodes (this operator's image).
    #[arg(
        long,
        env = "KHA_RELAY_IMAGE",
        default_value = "ghcr.io/safewords/kube-hardware-autoscaler:latest"
    )]
    relay_image: String,
}

/// The NodeScalingPool validating webhook (off unless `--webhook-addr` is set).
#[derive(clap::Args, Clone)]
struct WebhookArgs {
    /// Serve the validating webhook on this address (HTTPS) and register it.
    #[arg(long, env = "KHA_WEBHOOK_ADDR")]
    webhook_addr: Option<SocketAddr>,
    /// The Service through which the API server reaches the webhook.
    #[arg(
        long,
        env = "KHA_WEBHOOK_SERVICE",
        default_value = "kube-hardware-autoscaler-webhook"
    )]
    webhook_service: String,
    /// That Service's port.
    #[arg(long, env = "KHA_WEBHOOK_SERVICE_PORT", default_value_t = 443)]
    webhook_service_port: i32,
    /// Secret (in the operator's namespace) holding the webhook's CA and certificate.
    #[arg(
        long,
        env = "KHA_WEBHOOK_SECRET",
        default_value = "kube-hardware-autoscaler-webhook-tls"
    )]
    webhook_secret: String,
    /// Name of the ValidatingWebhookConfiguration the operator manages.
    #[arg(long, env = "KHA_WEBHOOK_CONFIG_NAME", default_value = "kube-hardware-autoscaler")]
    webhook_config_name: String,
    /// ClusterRole that owns the ValidatingWebhookConfiguration (deleting it deletes the configuration).
    #[arg(long, env = "KHA_WEBHOOK_OWNER_CLUSTER_ROLE")]
    webhook_owner_cluster_role: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the operator.
    Run {
        #[command(flatten)]
        drivers: DriverArgs,
        /// Address of the metrics and health endpoints.
        #[arg(long, env = "KHA_METRICS_ADDR", default_value = "0.0.0.0:8080")]
        metrics_addr: SocketAddr,
        /// Log power actions instead of executing them.
        #[arg(long, env = "KHA_DRY_RUN")]
        dry_run: bool,
        /// How much to say about scaling decisions: `quiet` (power actions only),
        /// `summary` (one line and an Event per decision, including why nothing
        /// is done) or `detailed` (also every machine's rank at info).
        #[arg(long, env = "KHA_DECISION_VERBOSITY", value_enum, default_value_t = DecisionVerbosity::Summary)]
        decision_verbosity: DecisionVerbosity,
        #[command(flatten)]
        webhook: WebhookArgs,
    },
    /// Send a Wake-on-LAN magic packet from this host (used by relay pods).
    Wake {
        /// MAC address of the NIC to wake.
        #[arg(long)]
        mac: String,
        /// UDP port.
        #[arg(long, default_value_t = 9)]
        port: u16,
        /// Broadcast address; defaults to every interface's broadcast address plus 255.255.255.255.
        #[arg(long)]
        broadcast: Option<String>,
    },
    /// Print the CustomResourceDefinitions as YAML.
    Crds,
    /// List the available power drivers.
    Drivers {
        /// Also print each driver's config JSON schema.
        #[arg(long)]
        schemas: bool,
    },
    /// Query or change the power state of a NodePowerManagementConfig directly (for testing interfaces and credentials).
    Power {
        /// NodePowerManagementConfig name.
        node_power_management_config: String,
        action: PowerAction,
        /// Use only this interface (by name, or driver name when unnamed) instead of the fallback chain.
        #[arg(long)]
        via: Option<String>,
        #[command(flatten)]
        drivers: DriverArgs,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum PowerAction {
    Status,
    On,
    Off,
    ForceOff,
}

fn init_logging(json: bool) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,kube_runtime=warn,kube_client=warn"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}

fn driver_context(client: Client, args: &DriverArgs) -> DriverContext {
    DriverContext {
        client,
        namespace: args.namespace.clone(),
        op_timeout: Duration::from_secs(args.op_timeout_seconds),
        shutdown_image: args
            .shutdown_image
            .clone()
            .filter(|i| !i.is_empty())
            .unwrap_or_else(|| args.relay_image.clone()),
        relay_image: args.relay_image.clone(),
    }
}

/// Starts a reflector for `api` and returns its store.
fn spawn_reflector<K>(api: Api<K>) -> Store<K>
where
    K: Resource<DynamicType = ()> + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
{
    let (reader, writer) = reflector::store();
    let stream = watcher(api, watcher::Config::default())
        .default_backoff()
        .modify(|o| o.managed_fields_mut().clear());
    let stream = reflector::reflector(writer, stream).applied_objects();
    tokio::spawn(async move {
        stream
            .for_each(|r| async move {
                if let Err(e) = r {
                    tracing::warn!(error = %e, "watch error");
                }
            })
            .await;
    });
    reader
}

async fn run(
    args: DriverArgs,
    metrics_addr: SocketAddr,
    dry_run: bool,
    decision_verbosity: DecisionVerbosity,
    webhook_args: WebhookArgs,
) -> anyhow::Result<()> {
    let client = Client::try_default().await.context("connecting to Kubernetes")?;
    let metrics = Metrics::new();
    tokio::spawn(metrics::serve(metrics_addr, metrics.clone()));

    let nodes = spawn_reflector(Api::<Node>::all(client.clone()));
    let pods = spawn_reflector(Api::<Pod>::all(client.clone()));
    let node_power_management_configs = spawn_reflector(Api::<NodePowerManagementConfig>::all(client.clone()));
    let pools = spawn_reflector(Api::<NodeScalingPool>::all(client.clone()));
    info!("waiting for caches to sync");
    nodes.wait_until_ready().await?;
    pods.wait_until_ready().await?;
    node_power_management_configs.wait_until_ready().await?;
    pools.wait_until_ready().await?;
    metrics.ready.store(true, Ordering::Relaxed);
    info!(dry_run, namespace = %args.namespace, "caches synced; starting controllers");

    // The webhook is registered only once it serves, and unregistered on shutdown.
    let webhook = webhook_args.webhook_addr.map(|addr| webhook::Settings {
        addr,
        namespace: args.namespace.clone(),
        service: webhook_args.webhook_service.clone(),
        service_port: webhook_args.webhook_service_port,
        secret: webhook_args.webhook_secret.clone(),
        config_name: webhook_args.webhook_config_name.clone(),
        owner_cluster_role: webhook_args.webhook_owner_cluster_role.clone(),
    });
    let webhook_shutdown = match &webhook {
        Some(settings) => {
            let tls = webhook::ensure_tls(&client, settings).await?;
            webhook::serve(settings.addr, &tls, nodes.clone()).await?;
            webhook::register(&client, settings, &tls).await?;
            // Unregister as soon as the pod is told to stop, before the controllers
            // wind down: a rollout should not leave pool changes refused.
            let (client, settings) = (client.clone(), settings.clone());
            Some(tokio::spawn(async move {
                shutdown_signal().await;
                webhook::unregister(&client, &settings).await;
            }))
        }
        None => None,
    };

    let reporter = Reporter {
        controller: "kube-hardware-autoscaler".into(),
        instance: std::env::var("POD_NAME").ok(),
    };
    let ctx = Arc::new(Context {
        client: client.clone(),
        drivers: driver_context(client.clone(), &args),
        nodes,
        pods,
        node_power_management_configs,
        pools,
        identity_cache: IdentityCache::default(),
        recorder: Recorder::new(client.clone(), reporter),
        metrics,
        dry_run,
        decision_verbosity,
        warned: Default::default(),
    });

    // NodePowerManagementConfig names equal their Node names, so a Node event maps 1:1 to
    // its NodePowerManagementConfig; any pool change can alter every machine's membership.
    let all_managed = ctx.node_power_management_configs.clone();
    let mn_controller = Controller::new(
        Api::<NodePowerManagementConfig>::all(client.clone()),
        watcher::Config::default(),
    )
    .watches(Api::<Node>::all(client.clone()), watcher::Config::default(), |node| {
        Some(ObjectRef::<NodePowerManagementConfig>::new(&node.name_any()))
    })
    .watches(
        Api::<NodeScalingPool>::all(client.clone()),
        watcher::Config::default(),
        move |_pool| {
            all_managed
                .state()
                .iter()
                .map(|mn| ObjectRef::from(mn.as_ref()))
                .collect::<Vec<_>>()
        },
    )
    .shutdown_on_signal()
    .run(
        node_power_management_config::reconcile,
        node_power_management_config::error_policy,
        ctx.clone(),
    )
    .for_each(|r| async move { log_controller_result("nodepowermanagementconfig", r) });
    let pool_controller = Controller::new(Api::<NodeScalingPool>::all(client.clone()), watcher::Config::default())
        .shutdown_on_signal()
        .run(
            node_scaling_pool::reconcile,
            node_scaling_pool::error_policy,
            ctx.clone(),
        )
        .for_each(|r| async move { log_controller_result("nodescalingpool", r) });
    tokio::join!(mn_controller, pool_controller);
    info!("controllers stopped");
    if let Some(task) = webhook_shutdown {
        let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
    }
    Ok(())
}

/// SIGTERM or Ctrl-C.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

async fn power(name: String, action: PowerAction, via: Option<String>, args: DriverArgs) -> anyhow::Result<()> {
    let client = Client::try_default().await.context("connecting to Kubernetes")?;
    let mn = Api::<NodePowerManagementConfig>::all(client.clone())
        .get(&name)
        .await
        .with_context(|| format!("fetching NodePowerManagementConfig {name}"))?;
    let ctx = driver_context(client, &args);
    if let Some(label) = via {
        // Exercise one specific interface, bypassing the fallback order.
        let pi = mn
            .spec
            .power_interfaces
            .iter()
            .find(|pi| pi.label() == label)
            .with_context(|| format!("NodePowerManagementConfig {name} has no power interface named {label:?}"))?;
        let driver = drivers::build_driver(&ctx, &mn.spec.node_name, pi).await?;
        match action {
            PowerAction::Status => println!("{name}: {} (via {label})", driver.power_state().await?),
            PowerAction::On => driver.power_on().await?,
            PowerAction::Off => driver.power_off(false).await?,
            PowerAction::ForceOff => driver.power_off(true).await?,
        }
        return Ok(());
    }
    let chain = PowerChain::build(&ctx, &mn).await?;
    let report = |via: &str, failures: &[String]| {
        for f in failures {
            eprintln!("  skipped {f}");
        }
        via.to_string()
    };
    match action {
        PowerAction::Status => {
            let o = chain.power_state().await?;
            let via = report(&o.via, &o.failures);
            println!("{name}: {} (via {via})", o.value);
        }
        PowerAction::On | PowerAction::Off | PowerAction::ForceOff => {
            let o = match action {
                PowerAction::On => chain.power_on().await?,
                PowerAction::Off => chain.power_off(false).await?,
                _ => chain.power_off(true).await?,
            };
            let via = report(&o.via, &o.failures);
            println!("{name}: done (via {via})");
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Several dependencies enable different rustls crypto backends (aws-lc-rs
    // via kube/reqwest, ring via rumqttc); pick one explicitly.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cli = Cli::parse();
    init_logging(cli.log_json);
    match cli.command {
        Command::Run {
            drivers,
            metrics_addr,
            dry_run,
            decision_verbosity,
            webhook,
        } => run(drivers, metrics_addr, dry_run, decision_verbosity, webhook).await,
        Command::Wake { mac, port, broadcast } => {
            let mac_bytes = kube_hardware_autoscaler::wake::parse_mac(&mac).context("invalid --mac")?;
            let targets = kube_hardware_autoscaler::wake::targets(broadcast.as_deref(), port)?;
            let sent = kube_hardware_autoscaler::wake::send(&mac_bytes, &targets, 3).await?;
            let list: Vec<String> = targets.iter().map(|t| t.to_string()).collect();
            info!(%mac, packets = sent, targets = %list.join(" "), "magic packet sent");
            Ok(())
        }
        Command::Crds => {
            // JSON documents (valid YAML): unquoted YAML would let YAML 1.1
            // parsers turn enum values such as `On`/`Off` into booleans.
            println!(
                "{}\n---\n{}",
                serde_json::to_string_pretty(&NodePowerManagementConfig::crd())?,
                serde_json::to_string_pretty(&NodeScalingPool::crd())?
            );
            Ok(())
        }
        Command::Drivers { schemas } => {
            for d in CATALOG {
                let creds = if d.requires_credentials() {
                    "credentials required"
                } else {
                    "no credentials"
                };
                println!("{:<10} {} ({creds})", d.name(), d.description());
                if schemas {
                    println!("{}\n", serde_json::to_string_pretty(&d.config_schema())?);
                }
            }
            Ok(())
        }
        Command::Power {
            node_power_management_config,
            action,
            via,
            drivers,
        } => power(node_power_management_config, action, via, drivers).await,
    }
}

/// Logs a controller result. Reconciles racing a deletion ("not found in
/// local store") are expected and only logged at debug level.
fn log_controller_result<K, E, W>(
    controller: &str,
    result: Result<(ObjectRef<K>, kube::runtime::controller::Action), kube::runtime::controller::Error<E, W>>,
) where
    K: Resource,
    E: std::error::Error,
    W: std::error::Error,
{
    match result {
        Ok(_) => {}
        Err(kube::runtime::controller::Error::ObjectNotFound(obj)) => {
            tracing::debug!(controller, object = %obj, "object deleted before reconcile");
        }
        Err(e) => error!(controller, error = %e, "controller error"),
    }
}
