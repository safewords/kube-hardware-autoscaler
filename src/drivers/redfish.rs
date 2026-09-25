//! DMTF Redfish driver (`ComputerSystem.Reset`).

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::OnceCell;

use super::{Credentials, DriverError, DriverInit, DriverKind, PowerDriver, Result, base_url, http_client};
use crate::crd::PowerState;

/// `config` of the `redfish` driver.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RedfishConfig {
    /// Base URL of the BMC, e.g. `https://10.0.0.10`.
    pub endpoint: String,
    /// Redfish ComputerSystem id. The first system is used when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_id: Option<String>,
    /// Skip TLS certificate verification (BMCs commonly use self-signed certs).
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

pub struct RedfishDriver {
    base: String,
    system_id: Option<String>,
    creds: Credentials,
    http: reqwest::Client,
    /// Resolved `@odata.id` of the ComputerSystem.
    system_path: OnceCell<String>,
}

impl DriverKind for RedfishDriver {
    const NAME: &'static str = "redfish";
    const DESCRIPTION: &'static str = "DMTF Redfish BMCs (iDRAC, iLO, XClarity, OpenBMC, ...)";
    type Config = RedfishConfig;

    fn build(config: RedfishConfig, init: &DriverInit<'_>) -> Result<Self> {
        Ok(Self {
            base: base_url(&config.endpoint)?,
            system_id: config.system_id,
            creds: init.credentials()?,
            http: http_client(config.insecure_skip_verify, init.ctx.op_timeout)?,
            system_path: OnceCell::new(),
        })
    }
}

impl RedfishDriver {
    async fn get(&self, path: &str) -> Result<Value> {
        let resp = self
            .http
            .get(format!("{}{}", self.base, path))
            .basic_auth(&self.creds.username, Some(&self.creds.password))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(DriverError::Interface(format!("GET {path} returned {status}: {body}")));
        }
        Ok(resp.json().await?)
    }

    async fn system_path(&self) -> Result<&String> {
        self.system_path
            .get_or_try_init(|| async {
                if let Some(id) = &self.system_id {
                    return Ok(format!("/redfish/v1/Systems/{id}"));
                }
                let systems = self.get("/redfish/v1/Systems").await?;
                first_member(&systems)
                    .ok_or_else(|| DriverError::Interface("no ComputerSystem found at /redfish/v1/Systems".into()))
            })
            .await
    }

    async fn reset(&self, reset_type: &str) -> Result<()> {
        let system = self.system_path().await?;
        let sys = self.get(system).await?;
        let target = reset_target(&sys).unwrap_or_else(|| format!("{system}/Actions/ComputerSystem.Reset"));
        let resp = self
            .http
            .post(format!("{}{}", self.base, target))
            .basic_auth(&self.creds.username, Some(&self.creds.password))
            .json(&json!({ "ResetType": reset_type }))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(DriverError::Interface(format!(
                "reset {reset_type} returned {status}: {body}"
            )));
        }
        Ok(())
    }
}

fn first_member(collection: &Value) -> Option<String> {
    collection["Members"].as_array()?.first()?["@odata.id"]
        .as_str()
        .map(String::from)
}

fn reset_target(system: &Value) -> Option<String> {
    system["Actions"]["#ComputerSystem.Reset"]["target"]
        .as_str()
        .map(String::from)
}

fn parse_power_state(system: &Value) -> PowerState {
    match system["PowerState"].as_str() {
        // Transitional states still draw power: "PoweringOn" is on its way up,
        // and "PoweringOff" is on until it reports Off (so a stuck graceful
        // shutdown can still be forced).
        Some("On") | Some("PoweringOn") | Some("PoweringOff") => PowerState::On,
        Some("Off") => PowerState::Off,
        _ => PowerState::Unknown,
    }
}

#[async_trait]
impl PowerDriver for RedfishDriver {
    async fn system_id(&self) -> Result<Option<crate::identity::SystemId>> {
        let system = self.system_path().await?;
        let sys = self.get(system).await?;
        Ok(sys["UUID"]
            .as_str()
            .map(|u| crate::identity::SystemId::Uuid(u.to_string())))
    }

    async fn power_state(&self) -> Result<PowerState> {
        let system = self.system_path().await?;
        let sys = self.get(system).await?;
        match parse_power_state(&sys) {
            PowerState::Unknown => Err(DriverError::Interface(format!(
                "unexpected PowerState: {}",
                sys["PowerState"]
            ))),
            s => Ok(s),
        }
    }

    async fn power_on(&self) -> Result<()> {
        self.reset("On").await
    }

    async fn power_off(&self, force: bool) -> Result<()> {
        self.reset(if force { "ForceOff" } else { "GracefulShutdown" }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_system() {
        let sys = json!({
            "PowerState": "On",
            "Actions": {"#ComputerSystem.Reset": {"target": "/redfish/v1/Systems/1/Actions/ComputerSystem.Reset"}}
        });
        assert_eq!(parse_power_state(&sys), PowerState::On);
        assert_eq!(
            reset_target(&sys).unwrap(),
            "/redfish/v1/Systems/1/Actions/ComputerSystem.Reset"
        );
        assert_eq!(parse_power_state(&json!({"PowerState": "Off"})), PowerState::Off);
        assert_eq!(parse_power_state(&json!({})), PowerState::Unknown);
        let col = json!({"Members": [{"@odata.id": "/redfish/v1/Systems/System.Embedded.1"}]});
        assert_eq!(first_member(&col).unwrap(), "/redfish/v1/Systems/System.Embedded.1");
    }
}
