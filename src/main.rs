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

use kube_hardware_autoscaler::controller::{Context, IdentityCache, node_power_management_config, node_scaling_pool};
use kube_hardware_autoscaler::crd::{NodePowerManagementConfig, NodeScalingPool};
use kube_hardware_autoscaler::drivers::{self, CATALOG, DriverContext, PowerChain};
use kube_hardware_autoscaler::metrics::{self, Metrics};

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
    /// Default image for Wake-on-LAN shutdown pods (must provide sh and nsenter).
    #[arg(long, env = "KHA_SHUTDOWN_IMAGE", default_value = "debian:stable-slim")]
    shutdown_image: String,
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
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,kube=warn"));
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
        shutdown_image: args.shutdown_image.clone(),
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

async fn run(args: DriverArgs, metrics_addr: SocketAddr, dry_run: bool) -> anyhow::Result<()> {
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
    Ok(())
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
        } => run(drivers, metrics_addr, dry_run).await,
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
