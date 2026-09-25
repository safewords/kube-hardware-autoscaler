//! Power driver catalog.
//!
//! Every management interface is a driver implementing two traits:
//!
//! * [`PowerDriver`] - the runtime operations (query state, power on/off);
//! * [`DriverKind`] - catalog metadata plus a typed `Config` and a constructor.
//!
//! Adding a new interface means writing one module that implements both
//! traits and adding one line to [`CATALOG`]. The `NodePowerManagementConfig` CRD stays
//! generic (`powerInterfaces[].driver` + free-form `config`), and each driver's
//! config is validated against its own type when the driver is built.

mod chain;
mod ipmi;
mod jetkvm;
mod nanokvm;
mod pikvm;
mod ping;
mod redfish;
mod wol;

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;

use crate::crd::{CredentialsRef, InterfaceAction, PowerInterface, PowerState};

pub use chain::{Outcome, PowerChain};
pub use ipmi::IpmiDriver;
pub use jetkvm::JetKvmDriver;
pub use nanokvm::NanoKvmDriver;
pub use pikvm::PiKvmDriver;
pub use ping::PingDriver;
pub use redfish::RedfishDriver;
pub use wol::WakeOnLanDriver;

/// All available drivers. Add new drivers here.
pub static CATALOG: &[&dyn DriverFactory] = &[
    &Entry::<IpmiDriver>(PhantomData),
    &Entry::<RedfishDriver>(PhantomData),
    &Entry::<PiKvmDriver>(PhantomData),
    &Entry::<JetKvmDriver>(PhantomData),
    &Entry::<NanoKvmDriver>(PhantomData),
    &Entry::<WakeOnLanDriver>(PhantomData),
    &Entry::<PingDriver>(PhantomData),
];

#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("invalid power interface configuration: {0}")]
    Config(String),
    #[error("credentials: {0}")]
    Credentials(String),
    #[error("kubernetes API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("management interface error: {0}")]
    Interface(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("operation timed out after {0:?}")]
    Timeout(Duration),
}

pub type Result<T, E = DriverError> = std::result::Result<T, E>;

/// Controls the power of a single machine.
#[async_trait]
pub trait PowerDriver: Send + Sync {
    /// Whether this driver can perform `action` at all (e.g. the `ping` driver
    /// only reports state). The chain skips drivers for unsupported actions.
    fn supports(&self, _action: InterfaceAction) -> bool {
        true
    }
    /// Queries the current power state.
    async fn power_state(&self) -> Result<PowerState>;
    /// Powers the machine on.
    async fn power_on(&self) -> Result<()>;
    /// Powers the machine off: gracefully (ACPI soft-off) unless `force` is set.
    async fn power_off(&self, force: bool) -> Result<()>;
    /// Identity of the machine this interface controls, if the interface can
    /// report one. Compared with the Node's SMBIOS UUID before power actions.
    async fn system_id(&self) -> Result<Option<crate::identity::SystemId>> {
        Ok(None)
    }
}

/// Catalog metadata and construction for a driver type.
pub trait DriverKind: PowerDriver + Sized + 'static {
    /// Name used in `spec.powerInterfaces[].driver`.
    const NAME: &'static str;
    /// One-line description shown by `kube-hardware-autoscaler drivers`.
    const DESCRIPTION: &'static str;
    /// Whether `spec.powerInterfaces[].credentialsSecretRef` is mandatory.
    const REQUIRES_CREDENTIALS: bool = true;
    /// Driver-specific configuration, deserialized from `spec.powerInterfaces[].config`.
    type Config: DeserializeOwned + JsonSchema;

    fn build(config: Self::Config, init: &DriverInit<'_>) -> Result<Self>;
}

/// Everything a driver may need to construct itself.
pub struct DriverInit<'a> {
    /// Credentials, when a `credentialsSecretRef` is configured.
    pub credentials: Option<Credentials>,
    /// Kubernetes node name of the machine.
    pub node_name: &'a str,
    pub ctx: &'a DriverContext,
}

impl DriverInit<'_> {
    /// Credentials for drivers that declare `REQUIRES_CREDENTIALS`.
    pub fn credentials(&self) -> Result<Credentials> {
        self.credentials
            .clone()
            .ok_or_else(|| DriverError::Config("credentialsSecretRef is required for this driver".into()))
    }
}

/// Object-safe view of a [`DriverKind`], used by the catalog.
pub trait DriverFactory: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn requires_credentials(&self) -> bool;
    /// JSON schema of the driver's `config`.
    fn config_schema(&self) -> serde_json::Value;
    /// Validates `config` without building the driver.
    fn validate(&self, config: &serde_json::Value) -> Result<()>;
    fn build(&self, config: &serde_json::Value, init: &DriverInit<'_>) -> Result<Box<dyn PowerDriver>>;
}

struct Entry<D>(PhantomData<fn() -> D>);

impl<D: DriverKind> Entry<D> {
    fn parse(config: &serde_json::Value) -> Result<D::Config> {
        serde_json::from_value(config.clone()).map_err(|e| DriverError::Config(format!("{} config: {e}", D::NAME)))
    }
}

impl<D: DriverKind> DriverFactory for Entry<D> {
    fn name(&self) -> &'static str {
        D::NAME
    }
    fn description(&self) -> &'static str {
        D::DESCRIPTION
    }
    fn requires_credentials(&self) -> bool {
        D::REQUIRES_CREDENTIALS
    }
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(schemars::schema_for!(D::Config)).unwrap_or_default()
    }
    fn validate(&self, config: &serde_json::Value) -> Result<()> {
        Self::parse(config).map(|_| ())
    }
    fn build(&self, config: &serde_json::Value, init: &DriverInit<'_>) -> Result<Box<dyn PowerDriver>> {
        Ok(Box::new(D::build(Self::parse(config)?, init)?))
    }
}

/// Looks a driver up by name.
pub fn lookup(name: &str) -> Result<&'static dyn DriverFactory> {
    CATALOG.iter().copied().find(|f| f.name() == name).ok_or_else(|| {
        let known: Vec<_> = CATALOG.iter().map(|f| f.name()).collect();
        DriverError::Config(format!("unknown driver {name:?} (available: {})", known.join(", ")))
    })
}

/// Username/password pair read from a Secret.
#[derive(Clone)]
pub struct Credentials {
    pub username: String,
    pub password: String,
    /// Any additional keys of the secret (e.g. `kgKey` for IPMI).
    pub extra: BTreeMap<String, String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Settings shared by all drivers.
#[derive(Clone)]
pub struct DriverContext {
    pub client: Client,
    /// Namespace credentials secrets (and shutdown pods) live in.
    pub namespace: String,
    /// Timeout for a single management interface operation.
    pub op_timeout: Duration,
    /// Default image for Wake-on-LAN shutdown pods.
    pub shutdown_image: String,
}

async fn load_credentials(ctx: &DriverContext, r: &CredentialsRef) -> Result<Credentials> {
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), &ctx.namespace);
    let secret = secrets
        .get_opt(&r.name)
        .await?
        .ok_or_else(|| DriverError::Credentials(format!("secret {}/{} not found", ctx.namespace, r.name)))?;
    let mut data: BTreeMap<String, String> = secret
        .data
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(k, v)| String::from_utf8(v.0).ok().map(|s| (k, s)))
        .collect();
    data.extend(secret.string_data.unwrap_or_default());
    let mut take = |key: &str| {
        data.remove(key)
            .ok_or_else(|| DriverError::Credentials(format!("secret {}/{} has no key {key:?}", ctx.namespace, r.name)))
    };
    let username = take(&r.username_key)?;
    let password = take(&r.password_key)?;
    Ok(Credentials {
        username,
        password,
        extra: data,
    })
}

/// Builds the driver for one power interface of a machine.
pub async fn build_driver(ctx: &DriverContext, node_name: &str, pi: &PowerInterface) -> Result<Box<dyn PowerDriver>> {
    let factory = lookup(&pi.driver)?;
    // Validate the config first so misconfigurations surface before any secret access.
    factory.validate(&pi.config)?;
    let credentials = match &pi.credentials_secret_ref {
        Some(r) => Some(load_credentials(ctx, r).await?),
        None if factory.requires_credentials() => {
            return Err(DriverError::Config(format!(
                "driver {} requires credentialsSecretRef",
                factory.name()
            )));
        }
        None => None,
    };
    let init = DriverInit {
        credentials,
        node_name,
        ctx,
    };
    factory.build(&pi.config, &init)
}

/// Builds a reqwest client for BMC-style HTTP APIs.
fn http_client(insecure: bool, timeout: Duration) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .tls_danger_accept_invalid_certs(insecure)
        .timeout(timeout)
        .user_agent(concat!("kube-hardware-autoscaler/", env!("CARGO_PKG_VERSION")))
        .build()?)
}

/// Validates an HTTP(S) base URL and strips trailing slashes.
fn base_url(endpoint: &str) -> Result<String> {
    let base = endpoint.trim_end_matches('/').to_string();
    if !base.starts_with("http://") && !base.starts_with("https://") {
        return Err(DriverError::Config(format!("endpoint must be an http(s) URL: {base}")));
    }
    Ok(base)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn catalog_names_are_unique_and_resolvable() {
        let mut names: Vec<_> = CATALOG.iter().map(|f| f.name()).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), CATALOG.len());
        for n in names {
            assert_eq!(lookup(n).unwrap().name(), n);
        }
        assert!(lookup("nope").is_err());
    }

    #[test]
    fn validates_configs_per_driver() {
        assert!(
            lookup("ipmi")
                .unwrap()
                .validate(&json!({"address": "10.0.0.5"}))
                .is_ok()
        );
        assert!(lookup("ipmi").unwrap().validate(&json!({})).is_err());
        assert!(
            lookup("ipmi")
                .unwrap()
                .validate(&json!({"address": "x", "bogus": 1}))
                .is_err()
        );
        assert!(
            lookup("redfish")
                .unwrap()
                .validate(&json!({"endpoint": "https://bmc"}))
                .is_ok()
        );
        assert!(
            lookup("pikvm")
                .unwrap()
                .validate(&json!({"endpoint": "https://kvm"}))
                .is_ok()
        );
        assert!(
            lookup("jetkvm")
                .unwrap()
                .validate(&json!({"broker": "mqtt://b", "baseTopic": "jetkvm/abc"}))
                .is_ok()
        );
        assert!(
            lookup("jetkvm")
                .unwrap()
                .validate(&json!({"broker": "mqtt://b"}))
                .is_err()
        );
        assert!(
            lookup("wakeOnLan")
                .unwrap()
                .validate(&json!({"macAddress": "aa:bb:cc:dd:ee:ff"}))
                .is_ok()
        );
        assert!(!lookup("wakeOnLan").unwrap().requires_credentials());
        assert!(
            lookup("ping")
                .unwrap()
                .validate(&json!({"address": "10.0.0.7", "method": "icmp"}))
                .is_ok()
        );
        assert!(
            lookup("ping")
                .unwrap()
                .validate(&json!({"address": "10.0.0.7", "method": "udp"}))
                .is_err()
        );
        assert!(!lookup("ping").unwrap().requires_credentials());
    }

    #[test]
    fn schemas_are_objects() {
        for f in CATALOG {
            assert_eq!(f.config_schema()["type"], "object", "{}", f.name());
        }
    }
}
