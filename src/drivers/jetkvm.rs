//! JetKVM driver, via JetKVM's built-in MQTT integration.
//!
//! JetKVM has no HTTP API for power control (its RPC runs over a WebRTC data
//! channel), but it can connect to an MQTT broker and exposes the ATX and DC
//! power extensions there:
//!
//! * state (retained): `{base}/atx/state` `{"power":bool,"hdd":bool}` and
//!   `{base}/dc/state` `{"isOn":bool,...}`; availability on `{base}/status`
//!   `{"online":bool}`;
//! * commands: `{base}/atx_power_short/set` (500 ms press),
//!   `{base}/atx_power_long/set` (5 s press) and `{base}/dc_power/set`
//!   (`ON`/`OFF`).
//!
//! On the JetKVM, enable MQTT and "Enable actions" in the settings. The ATX
//! power button toggles, so the driver always reads the state first and only
//! presses the button when the machine is not already in the requested state.

use std::time::Duration;

use async_trait::async_trait;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS, Transport};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{Credentials, DriverError, DriverInit, DriverKind, PowerDriver, Result};
use crate::crd::PowerState;

/// `config` of the `jetkvm` driver. `credentialsSecretRef`, when set, holds
/// the MQTT broker credentials.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JetKvmConfig {
    /// Broker URL: `mqtt://host:1883` or `mqtts://host:8883` (system roots).
    pub broker: String,
    /// The JetKVM's MQTT base topic including its device id, e.g. `jetkvm/abc123`
    /// (JetKVM appends the device id to the configured base topic).
    pub base_topic: String,
    /// Which JetKVM extension controls the machine.
    #[serde(default)]
    pub extension: JetKvmExtension,
    /// How long to wait for the retained state after subscribing, in milliseconds.
    #[serde(default = "default_state_timeout")]
    pub state_timeout_ms: u64,
}

fn default_state_timeout() -> u64 {
    5000
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum JetKvmExtension {
    /// ATX power board wired to the motherboard's power button and LED.
    #[default]
    Atx,
    /// DC power extension switching the machine's DC supply. Only hard power
    /// off is possible: graceful power off fails so that the next interface in
    /// `powerInterfaces` (e.g. a Wake-on-LAN shutdown pod) is used instead.
    Dc,
}

pub struct JetKvmDriver {
    config: JetKvmConfig,
    host: String,
    port: u16,
    tls: bool,
    creds: Option<Credentials>,
}

impl DriverKind for JetKvmDriver {
    const NAME: &'static str = "jetkvm";
    const DESCRIPTION: &'static str = "JetKVM ATX/DC power extension via its MQTT integration";
    const REQUIRES_CREDENTIALS: bool = false;
    type Config = JetKvmConfig;

    fn build(config: JetKvmConfig, init: &DriverInit<'_>) -> Result<Self> {
        let (host, port, tls) = parse_broker(&config.broker)?;
        let base = config.base_topic.trim_end_matches('/');
        if base.is_empty() || base.contains(['#', '+']) {
            return Err(DriverError::Config(format!(
                "invalid baseTopic {:?}",
                config.base_topic
            )));
        }
        let config = JetKvmConfig {
            base_topic: base.to_string(),
            ..config
        };
        Ok(Self {
            config,
            host,
            port,
            tls,
            creds: init.credentials.clone(),
        })
    }
}

fn parse_broker(url: &str) -> Result<(String, u16, bool)> {
    let (tls, rest, default_port) = if let Some(r) = url.strip_prefix("mqtts://").or(url.strip_prefix("ssl://")) {
        (true, r, 8883)
    } else if let Some(r) = url.strip_prefix("mqtt://").or(url.strip_prefix("tcp://")) {
        (false, r, 1883)
    } else {
        return Err(DriverError::Config(format!(
            "broker must be an mqtt:// or mqtts:// URL: {url}"
        )));
    };
    let rest = rest.trim_end_matches('/');
    let bad_port = || DriverError::Config(format!("invalid broker port in {url}"));
    let (host, port) = if let Some(bracketed) = rest.strip_prefix('[') {
        // IPv6 literal: [addr] or [addr]:port
        let (h, after) = bracketed.split_once(']').ok_or_else(bad_port)?;
        let port = match after.strip_prefix(':') {
            Some(p) => p.parse().map_err(|_| bad_port())?,
            None if after.is_empty() => default_port,
            None => return Err(bad_port()),
        };
        (h.to_string(), port)
    } else {
        match rest.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse().map_err(|_| bad_port())?),
            None => (rest.to_string(), default_port),
        }
    };
    if host.is_empty() {
        return Err(DriverError::Config(format!("broker URL has no host: {url}")));
    }
    Ok((host, port, tls))
}

/// Extracts the power state from a state topic payload.
fn parse_state(extension: JetKvmExtension, payload: &[u8]) -> Option<bool> {
    let v: Value = serde_json::from_slice(payload).ok()?;
    match extension {
        JetKvmExtension::Atx => v["power"].as_bool(),
        JetKvmExtension::Dc => v["isOn"].as_bool(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    On,
    Off { force: bool },
}

/// Decides which command (topic suffix, payload) to publish, if any, given
/// the current power state. Pressing the ATX button toggles power, so nothing
/// is sent when the machine is already in the requested state.
fn command_for(extension: JetKvmExtension, op: Op, is_on: bool) -> Result<Option<(&'static str, &'static str)>> {
    Ok(match (extension, op, is_on) {
        (_, Op::On, true) | (_, Op::Off { .. }, false) => None,
        (JetKvmExtension::Atx, Op::On, false) => Some(("atx_power_short/set", "PRESS")),
        (JetKvmExtension::Atx, Op::Off { force: false }, true) => Some(("atx_power_short/set", "PRESS")),
        (JetKvmExtension::Atx, Op::Off { force: true }, true) => Some(("atx_power_long/set", "PRESS")),
        (JetKvmExtension::Dc, Op::On, false) => Some(("dc_power/set", "ON")),
        (JetKvmExtension::Dc, Op::Off { force: true }, true) => Some(("dc_power/set", "OFF")),
        (JetKvmExtension::Dc, Op::Off { force: false }, true) => {
            return Err(DriverError::Interface(
                "JetKVM DC extension cannot shut down gracefully; use another interface for graceful power off".into(),
            ));
        }
    })
}

impl JetKvmDriver {
    fn state_topic(&self) -> String {
        match self.config.extension {
            JetKvmExtension::Atx => format!("{}/atx/state", self.config.base_topic),
            JetKvmExtension::Dc => format!("{}/dc/state", self.config.base_topic),
        }
    }

    /// Connects, reads the retained power state and, if `op` is given, sends
    /// the matching command. Returns the power state observed before acting.
    async fn exchange(&self, op: Option<Op>) -> Result<bool> {
        let client_id = format!("kube-hardware-autoscaler-{:08x}", rand_suffix());
        let mut opts = MqttOptions::new(client_id, &self.host, self.port);
        opts.set_keep_alive(Duration::from_secs(10)).set_clean_session(true);
        if self.tls {
            opts.set_transport(Transport::tls_with_default_config());
        }
        if let Some(c) = &self.creds {
            opts.set_credentials(&c.username, &c.password);
        }
        let (client, mut eventloop) = AsyncClient::new(opts, 10);
        let state_topic = self.state_topic();
        let status_topic = format!("{}/status", self.config.base_topic);
        let mqtt_err = |e: rumqttc::ClientError| DriverError::Interface(format!("mqtt: {e}"));
        client
            .subscribe(&state_topic, QoS::AtLeastOnce)
            .await
            .map_err(mqtt_err)?;
        client
            .subscribe(&status_topic, QoS::AtLeastOnce)
            .await
            .map_err(mqtt_err)?;

        let state_timeout = Duration::from_millis(self.config.state_timeout_ms);
        let result = async {
            // 1. Wait for the retained state.
            let deadline = tokio::time::Instant::now() + state_timeout;
            let is_on = loop {
                let event = tokio::time::timeout_at(deadline, eventloop.poll())
                    .await
                    .map_err(|_| {
                        DriverError::Interface(format!(
                            "no retained state on {state_topic} within {state_timeout:?} (is MQTT enabled on the JetKVM and the extension active?)"
                        ))
                    })?
                    .map_err(|e| DriverError::Interface(format!("mqtt connection: {e}")))?;
                if let Event::Incoming(Incoming::Publish(p)) = event {
                    if p.topic == status_topic
                        && serde_json::from_slice::<Value>(&p.payload).ok().and_then(|v| v["online"].as_bool()) == Some(false)
                    {
                        return Err(DriverError::Interface("JetKVM reports itself offline".into()));
                    }
                    if p.topic == state_topic {
                        break parse_state(self.config.extension, &p.payload).ok_or_else(|| {
                            DriverError::Interface(format!("unexpected payload on {state_topic}: {}", String::from_utf8_lossy(&p.payload)))
                        })?;
                    }
                }
            };

            // 2. Send the command, if one is needed, and wait for the broker's ack.
            if let Some(op) = op
                && let Some((suffix, payload)) = command_for(self.config.extension, op, is_on)?
            {
                let topic = format!("{}/{suffix}", self.config.base_topic);
                client.publish(&topic, QoS::AtLeastOnce, false, payload).await.map_err(mqtt_err)?;
                let deadline = tokio::time::Instant::now() + state_timeout;
                loop {
                    let event = tokio::time::timeout_at(deadline, eventloop.poll())
                        .await
                        .map_err(|_| DriverError::Interface(format!("broker did not acknowledge publish to {topic}")))?
                        .map_err(|e| DriverError::Interface(format!("mqtt connection: {e}")))?;
                    if let Event::Incoming(Incoming::PubAck(_)) = event {
                        break;
                    }
                }
            }
            Ok(is_on)
        }
        .await;

        let _ = client.disconnect().await;
        // Flush the disconnect; errors are irrelevant at this point.
        let _ = tokio::time::timeout(Duration::from_millis(200), eventloop.poll()).await;
        result
    }
}

fn rand_suffix() -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    h.finish() as u32
}

#[async_trait]
impl PowerDriver for JetKvmDriver {
    async fn power_state(&self) -> Result<PowerState> {
        Ok(if self.exchange(None).await? {
            PowerState::On
        } else {
            PowerState::Off
        })
    }

    async fn power_on(&self) -> Result<()> {
        self.exchange(Some(Op::On)).await.map(|_| ())
    }

    async fn power_off(&self, force: bool) -> Result<()> {
        self.exchange(Some(Op::Off { force })).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_broker_urls() {
        assert_eq!(
            parse_broker("mqtt://10.0.0.2").unwrap(),
            ("10.0.0.2".into(), 1883, false)
        );
        assert_eq!(
            parse_broker("mqtts://broker.lan:8884/").unwrap(),
            ("broker.lan".into(), 8884, true)
        );
        assert_eq!(
            parse_broker("tcp://[fd00::1]:1884").unwrap(),
            ("fd00::1".into(), 1884, false)
        );
        assert!(parse_broker("http://x").is_err());
        assert!(parse_broker("mqtt://host:nope").is_err());
    }

    #[test]
    fn parses_state_payloads() {
        assert_eq!(
            parse_state(JetKvmExtension::Atx, br#"{"power":true,"hdd":false}"#),
            Some(true)
        );
        assert_eq!(
            parse_state(JetKvmExtension::Atx, br#"{"power":false,"hdd":true}"#),
            Some(false)
        );
        assert_eq!(
            parse_state(JetKvmExtension::Dc, br#"{"isOn":true,"voltage":12.1}"#),
            Some(true)
        );
        assert_eq!(parse_state(JetKvmExtension::Atx, b"garbage"), None);
    }

    #[test]
    fn never_toggles_a_machine_already_in_the_target_state() {
        use JetKvmExtension::*;
        assert_eq!(command_for(Atx, Op::On, true).unwrap(), None);
        assert_eq!(command_for(Atx, Op::Off { force: true }, false).unwrap(), None);
        assert_eq!(
            command_for(Atx, Op::On, false).unwrap(),
            Some(("atx_power_short/set", "PRESS"))
        );
        assert_eq!(
            command_for(Atx, Op::Off { force: false }, true).unwrap(),
            Some(("atx_power_short/set", "PRESS"))
        );
        assert_eq!(
            command_for(Atx, Op::Off { force: true }, true).unwrap(),
            Some(("atx_power_long/set", "PRESS"))
        );
        assert_eq!(command_for(Dc, Op::On, false).unwrap(), Some(("dc_power/set", "ON")));
        assert_eq!(
            command_for(Dc, Op::Off { force: true }, true).unwrap(),
            Some(("dc_power/set", "OFF"))
        );
        assert!(command_for(Dc, Op::Off { force: false }, true).is_err());
    }
}
