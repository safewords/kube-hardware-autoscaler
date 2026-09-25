//! PiKVM ATX power driver (`/api/atx`).

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{Credentials, DriverError, DriverInit, DriverKind, PowerDriver, Result, base_url, http_client};
use crate::crd::PowerState;

/// `config` of the `pikvm` driver.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PiKvmConfig {
    /// Base URL of the KVM, e.g. `https://pikvm.local`.
    pub endpoint: String,
    /// Skip TLS certificate verification.
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

pub struct PiKvmDriver {
    base: String,
    creds: Credentials,
    http: reqwest::Client,
}

impl DriverKind for PiKvmDriver {
    const NAME: &'static str = "pikvm";
    const DESCRIPTION: &'static str = "PiKVM (IP-KVM) ATX power control";
    type Config = PiKvmConfig;

    fn build(config: PiKvmConfig, init: &DriverInit<'_>) -> Result<Self> {
        Ok(Self {
            base: base_url(&config.endpoint)?,
            creds: init.credentials()?,
            http: http_client(config.insecure_skip_verify, init.ctx.op_timeout)?,
        })
    }
}

impl PiKvmDriver {
    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}{}", self.base, path))
            .header("X-KVMD-User", &self.creds.username)
            .header("X-KVMD-Passwd", &self.creds.password)
    }

    async fn send(&self, rb: reqwest::RequestBuilder, what: &str) -> Result<Value> {
        let resp = rb.send().await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() || body["ok"] != Value::Bool(true) {
            return Err(DriverError::Interface(format!("{what} returned {status}: {body}")));
        }
        Ok(body)
    }

    async fn action(&self, action: &str) -> Result<()> {
        let rb = self.request(reqwest::Method::POST, &format!("/api/atx/power?action={action}"));
        self.send(rb, &format!("atx power {action}")).await.map(|_| ())
    }
}

fn parse_atx(body: &Value) -> PowerState {
    match body["result"]["leds"]["power"].as_bool() {
        Some(true) => PowerState::On,
        Some(false) => PowerState::Off,
        None => PowerState::Unknown,
    }
}

#[async_trait]
impl PowerDriver for PiKvmDriver {
    async fn power_state(&self) -> Result<PowerState> {
        let body = self
            .send(self.request(reqwest::Method::GET, "/api/atx"), "atx state")
            .await?;
        match parse_atx(&body) {
            PowerState::Unknown => Err(DriverError::Interface(format!("unexpected atx state: {body}"))),
            s => Ok(s),
        }
    }

    async fn power_on(&self) -> Result<()> {
        self.action("on").await
    }

    async fn power_off(&self, force: bool) -> Result<()> {
        self.action(if force { "off_hard" } else { "off" }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_atx_state() {
        let on = json!({"ok": true, "result": {"enabled": true, "leds": {"power": true, "hdd": false}}});
        let off = json!({"ok": true, "result": {"enabled": true, "leds": {"power": false, "hdd": false}}});
        assert_eq!(parse_atx(&on), PowerState::On);
        assert_eq!(parse_atx(&off), PowerState::Off);
        assert_eq!(parse_atx(&json!({"ok": true})), PowerState::Unknown);
    }
}
