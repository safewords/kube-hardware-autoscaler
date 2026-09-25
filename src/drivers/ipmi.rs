//! IPMI v2.0 (RMCP+) driver.
//!
//! Speaks IPMI over LAN directly via the [`ipmi`] crate (RAKP-HMAC-SHA1
//! authentication, HMAC-SHA1-96 integrity, AES-CBC-128 confidentiality -
//! cipher suite 3). No external tools are required. A session is opened per
//! operation and closed afterwards, so no BMC session slots are held.

use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use ipmi::blocking::Client;
use ipmi::{ChassisControl, PrivilegeLevel};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Credentials, DriverError, DriverInit, DriverKind, PowerDriver, Result};
use crate::crd::PowerState;

/// `config` of the `ipmi` driver. The credentials secret may carry an
/// additional `kgKey` entry holding the BMC key (Kg) for two-key logins.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IpmiConfig {
    /// BMC host name or IP address.
    pub address: String,
    /// BMC UDP port. Defaults to 623.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Session privilege level. Power control needs at least OPERATOR.
    #[serde(default)]
    pub privilege_level: IpmiPrivilege,
    /// Timeout of a single UDP request, in milliseconds. Defaults to 2000.
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
    /// Send attempts per request (including the first). Defaults to 3.
    #[serde(default = "default_attempts")]
    pub attempts: u32,
}

fn default_port() -> u16 {
    623
}
fn default_request_timeout() -> u64 {
    2000
}
fn default_attempts() -> u32 {
    3
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum IpmiPrivilege {
    Operator,
    #[default]
    Administrator,
}

impl From<IpmiPrivilege> for PrivilegeLevel {
    fn from(p: IpmiPrivilege) -> Self {
        match p {
            IpmiPrivilege::Operator => PrivilegeLevel::Operator,
            IpmiPrivilege::Administrator => PrivilegeLevel::Administrator,
        }
    }
}

pub struct IpmiDriver {
    config: IpmiConfig,
    creds: Credentials,
    op_timeout: Duration,
}

impl DriverKind for IpmiDriver {
    const NAME: &'static str = "ipmi";
    const DESCRIPTION: &'static str = "IPMI v2.0 over LAN (RMCP+, cipher suite 3), native implementation";
    type Config = IpmiConfig;

    fn build(config: IpmiConfig, init: &DriverInit<'_>) -> Result<Self> {
        if config.address.is_empty() {
            return Err(DriverError::Config("ipmi address must not be empty".into()));
        }
        Ok(Self {
            config,
            creds: init.credentials()?,
            op_timeout: init.ctx.op_timeout,
        })
    }
}

fn ipmi_err(e: ipmi::Error) -> DriverError {
    DriverError::Interface(format!("ipmi: {e}"))
}

impl IpmiDriver {
    async fn resolve(&self) -> Result<SocketAddr> {
        let mut addrs = tokio::net::lookup_host((self.config.address.as_str(), self.config.port)).await?;
        addrs
            .next()
            .ok_or_else(|| DriverError::Interface(format!("cannot resolve {}", self.config.address)))
    }

    /// Opens an authenticated RMCP+ session, runs `op`, and closes the session.
    ///
    /// The `ipmi` client is synchronous here (its async futures are not
    /// `Send`), so the whole exchange runs on the blocking thread pool and is
    /// bounded by the driver timeout.
    async fn session<T: Send + 'static>(
        &self,
        op: impl FnOnce(&Client) -> ipmi::Result<T> + Send + 'static,
    ) -> Result<T> {
        let target = self.resolve().await?;
        let mut builder = Client::builder(target)
            .username(&self.creds.username)
            .password(&self.creds.password)
            .privilege_level(self.config.privilege_level.into())
            .timeout(Duration::from_millis(self.config.request_timeout_ms))
            .retries(self.config.attempts.max(1));
        if let Some(kg) = self.creds.extra.get("kgKey") {
            builder = builder.bmc_key(kg);
        }
        let task = tokio::task::spawn_blocking(move || {
            let client = builder.build()?;
            let result = op(&client);
            if let Err(e) = client.close_session() {
                tracing::debug!(error = %e, "failed to close IPMI session");
            }
            result
        });
        tokio::time::timeout(self.op_timeout, task)
            .await
            .map_err(|_| DriverError::Timeout(self.op_timeout))?
            .map_err(|e| DriverError::Interface(format!("ipmi worker failed: {e}")))?
            .map_err(ipmi_err)
    }

    async fn control(&self, action: ChassisControl) -> Result<()> {
        self.session(move |c| c.chassis_control(action)).await
    }
}

#[async_trait]
impl PowerDriver for IpmiDriver {
    async fn system_id(&self) -> Result<Option<crate::identity::SystemId>> {
        let guid = self.session(|c| c.get_system_guid().map(|g| g.bytes)).await?;
        Ok(Some(crate::identity::SystemId::RawGuid(guid)))
    }

    async fn power_state(&self) -> Result<PowerState> {
        let on = self
            .session(|c| c.get_chassis_status().map(|s| s.system_power_on))
            .await?;
        Ok(if on { PowerState::On } else { PowerState::Off })
    }

    async fn power_on(&self) -> Result<()> {
        self.control(ChassisControl::PowerUp).await
    }

    async fn power_off(&self, force: bool) -> Result<()> {
        self.control(if force {
            ChassisControl::PowerDown
        } else {
            ChassisControl::AcpiSoft
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let c: IpmiConfig = serde_json::from_value(serde_json::json!({"address": "10.0.0.5"})).unwrap();
        assert_eq!(c.port, 623);
        assert_eq!(c.privilege_level, IpmiPrivilege::Administrator);
        assert_eq!(c.attempts, 3);
        assert!(serde_json::from_value::<IpmiConfig>(serde_json::json!({"address": "x", "interface": "lan"})).is_err());
    }
}
