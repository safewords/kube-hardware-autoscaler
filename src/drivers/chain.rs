//! Ordered fallback across a machine's power interfaces.
//!
//! Every operation walks `spec.powerInterfaces` in priority order, skipping
//! interfaces not enabled for that operation, bounding each attempt by the
//! interface's timeout, and returns the first success. Interfaces that cannot
//! even be built (missing secret, bad config) are reported but do not prevent
//! the others from being used.

use std::time::Duration;

use futures::future::BoxFuture;

use super::{DriverContext, DriverError, PowerDriver, Result, build_driver};
use crate::crd::{InterfaceAction, NodePowerManagementConfig, PowerInterface, PowerState};
use crate::identity::{self, IdentityCheck};

struct Link {
    label: String,
    interface: PowerInterface,
    driver: Box<dyn PowerDriver>,
    timeout: Duration,
}

pub struct PowerChain {
    links: Vec<Link>,
    /// Interfaces that could not be built, as "label: error".
    unavailable: Vec<String>,
}

/// Result of a successful chain operation.
#[derive(Debug)]
pub struct Outcome<T> {
    pub value: T,
    /// Label of the interface that succeeded.
    pub via: String,
    /// Interfaces that were tried first and failed, as "label: error".
    pub failures: Vec<String>,
}

impl PowerChain {
    /// Builds all interfaces of a machine. Fails only if none can be built.
    pub async fn build(ctx: &DriverContext, mn: &NodePowerManagementConfig) -> Result<Self> {
        let interfaces = &mn.spec.power_interfaces;
        if interfaces.is_empty() {
            return Err(DriverError::Config("spec.powerInterfaces must not be empty".into()));
        }
        let mut links = Vec::new();
        let mut unavailable = Vec::new();
        for pi in interfaces {
            let label = pi.label().to_string();
            match build_driver(ctx, &mn.spec.node_name, pi).await {
                Ok(driver) => links.push(Link {
                    label,
                    interface: pi.clone(),
                    driver,
                    timeout: pi.timeout_seconds.map(Duration::from_secs).unwrap_or(ctx.op_timeout),
                }),
                Err(e) => unavailable.push(format!("{label}: {e}")),
            }
        }
        if links.is_empty() {
            return Err(DriverError::Config(unavailable.join("; ")));
        }
        Ok(Self { links, unavailable })
    }

    #[cfg(test)]
    fn from_parts(links: Vec<(PowerInterface, Box<dyn PowerDriver>, Duration)>) -> Self {
        let links = links
            .into_iter()
            .map(|(interface, driver, timeout)| Link {
                label: interface.label().to_string(),
                interface,
                driver,
                timeout,
            })
            .collect();
        Self {
            links,
            unavailable: vec![],
        }
    }

    /// Interfaces that could not be built, or were disabled by an identity mismatch.
    pub fn unavailable(&self) -> &[String] {
        &self.unavailable
    }

    /// Asks every interface which machine it controls and compares it with the
    /// Node's SMBIOS UUID. Returns (label, result, cacheable); errors are
    /// reported as `Unverifiable` and not cacheable, so they are retried.
    pub async fn check_identity(&self, node_uuid: Option<&str>) -> Vec<(String, IdentityCheck, bool)> {
        let mut out = Vec::new();
        for link in &self.links {
            let result = tokio::time::timeout(link.timeout, link.driver.system_id())
                .await
                .unwrap_or(Err(DriverError::Timeout(link.timeout)));
            match result {
                Ok(id) => out.push((link.label.clone(), identity::check(id.as_ref(), node_uuid), true)),
                Err(e) => {
                    tracing::debug!(interface = %link.label, error = %e, "identity check failed");
                    out.push((link.label.clone(), IdentityCheck::Unverifiable, false));
                }
            }
        }
        out
    }

    /// Disables every interface whose identity check found a different
    /// machine: it is never used again for any operation in this chain.
    pub fn disable_mismatched(&mut self, results: &[(String, IdentityCheck, bool)]) {
        let bad: Vec<(&str, String)> = results
            .iter()
            .filter_map(|(label, r, _)| match r {
                IdentityCheck::Mismatch { reported, expected } => Some((
                    label.as_str(),
                    format!("{label}: controls a different machine (reports {reported}, node is {expected}); disabled"),
                )),
                _ => None,
            })
            .collect();
        for (label, why) in bad {
            self.links.retain(|l| l.label != label);
            self.unavailable.push(why);
        }
    }

    async fn attempt<T: Send>(
        &self,
        action: InterfaceAction,
        op: impl for<'a> Fn(&'a dyn PowerDriver) -> BoxFuture<'a, Result<T>>,
    ) -> Result<Outcome<T>> {
        let mut failures = self.unavailable.clone();
        let mut tried = false;
        for link in self.links.iter().filter(|l| l.interface.handles(action)) {
            tried = true;
            let result = tokio::time::timeout(link.timeout, op(link.driver.as_ref()))
                .await
                .unwrap_or(Err(DriverError::Timeout(link.timeout)));
            match result {
                Ok(value) => {
                    return Ok(Outcome {
                        value,
                        via: link.label.clone(),
                        failures,
                    });
                }
                Err(e) => {
                    tracing::debug!(interface = %link.label, ?action, error = %e, "power interface failed; trying next");
                    failures.push(format!("{}: {e}", link.label));
                }
            }
        }
        if !tried {
            failures.push(format!("no power interface is enabled for {action:?}"));
        }
        Err(DriverError::Interface(format!(
            "all power interfaces failed ({})",
            failures.join("; ")
        )))
    }

    pub async fn power_state(&self) -> Result<Outcome<PowerState>> {
        self.attempt(InterfaceAction::Status, |d| {
            Box::pin(async move {
                match d.power_state().await? {
                    PowerState::Unknown => Err(DriverError::Interface("power state unknown".into())),
                    s => Ok(s),
                }
            })
        })
        .await
    }

    pub async fn power_on(&self) -> Result<Outcome<()>> {
        self.attempt(InterfaceAction::PowerOn, |d| d.power_on()).await
    }

    pub async fn power_off(&self, force: bool) -> Result<Outcome<()>> {
        self.attempt(InterfaceAction::PowerOff, move |d| d.power_off(force))
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use async_trait::async_trait;

    use super::*;

    enum Behaviour {
        Ok(PowerState),
        Fail,
        Hang,
    }

    struct Fake {
        behaviour: Behaviour,
        calls: Arc<AtomicU32>,
    }

    impl Fake {
        async fn run(&self) -> Result<PowerState> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.behaviour {
                Behaviour::Ok(s) => Ok(s),
                Behaviour::Fail => Err(DriverError::Interface("boom".into())),
                Behaviour::Hang => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    unreachable!()
                }
            }
        }
    }

    #[async_trait]
    impl PowerDriver for Fake {
        async fn power_state(&self) -> Result<PowerState> {
            self.run().await
        }
        async fn power_on(&self) -> Result<()> {
            self.run().await.map(|_| ())
        }
        async fn power_off(&self, _force: bool) -> Result<()> {
            self.run().await.map(|_| ())
        }
    }

    fn iface(name: &str, actions: Option<Vec<InterfaceAction>>) -> PowerInterface {
        PowerInterface {
            name: Some(name.into()),
            driver: "fake".into(),
            actions,
            timeout_seconds: None,
            credentials_secret_ref: None,
            config: serde_json::json!({}),
        }
    }

    fn link(
        name: &str,
        b: Behaviour,
        actions: Option<Vec<InterfaceAction>>,
    ) -> (PowerInterface, Box<dyn PowerDriver>, Duration, Arc<AtomicU32>) {
        let calls = Arc::new(AtomicU32::new(0));
        let d: Box<dyn PowerDriver> = Box::new(Fake {
            behaviour: b,
            calls: calls.clone(),
        });
        (iface(name, actions), d, Duration::from_millis(50), calls)
    }

    #[tokio::test]
    async fn first_success_wins_and_later_interfaces_are_not_called() {
        let (i1, d1, t1, c1) = link("ipmi", Behaviour::Ok(PowerState::On), None);
        let (i2, d2, t2, c2) = link("wol", Behaviour::Ok(PowerState::Off), None);
        let chain = PowerChain::from_parts(vec![(i1, d1, t1), (i2, d2, t2)]);
        let out = chain.power_state().await.unwrap();
        assert_eq!((out.value, out.via.as_str()), (PowerState::On, "ipmi"));
        assert_eq!((c1.load(Ordering::SeqCst), c2.load(Ordering::SeqCst)), (1, 0));
    }

    #[tokio::test]
    async fn falls_back_on_failure_and_timeout() {
        let (i1, d1, t1, _) = link("hangs", Behaviour::Hang, None);
        let (i2, d2, t2, _) = link("fails", Behaviour::Fail, None);
        let (i3, d3, t3, _) = link("works", Behaviour::Ok(PowerState::On), None);
        let chain = PowerChain::from_parts(vec![(i1, d1, t1), (i2, d2, t2), (i3, d3, t3)]);
        let out = chain.power_on().await.unwrap();
        assert_eq!(out.via, "works");
        assert_eq!(out.failures.len(), 2);
        assert!(out.failures[0].starts_with("hangs: operation timed out"));
        assert!(out.failures[1].starts_with("fails:"));
    }

    #[tokio::test]
    async fn respects_per_interface_actions() {
        // IPMI for status/off, Wake-on-LAN only for power on.
        let (i1, d1, t1, c1) = link(
            "ipmi",
            Behaviour::Fail,
            Some(vec![InterfaceAction::Status, InterfaceAction::PowerOff]),
        );
        let (i2, d2, t2, c2) = link(
            "wol",
            Behaviour::Ok(PowerState::Off),
            Some(vec![InterfaceAction::PowerOn]),
        );
        let chain = PowerChain::from_parts(vec![(i1, d1, t1), (i2, d2, t2)]);
        assert_eq!(chain.power_on().await.unwrap().via, "wol");
        assert_eq!(c1.load(Ordering::SeqCst), 0, "ipmi is not used for power on");
        assert!(chain.power_state().await.is_err(), "wol is not used for status");
        assert_eq!(c2.load(Ordering::SeqCst), 1);
        let err = chain.power_off(false).await.unwrap_err().to_string();
        assert!(
            err.contains("all power interfaces failed") && err.contains("ipmi: "),
            "{err}"
        );
    }

    #[tokio::test]
    async fn unknown_state_falls_through() {
        let (i1, d1, t1, _) = link("a", Behaviour::Ok(PowerState::Unknown), None);
        let (i2, d2, t2, _) = link("b", Behaviour::Ok(PowerState::Off), None);
        let chain = PowerChain::from_parts(vec![(i1, d1, t1), (i2, d2, t2)]);
        let out = chain.power_state().await.unwrap();
        assert_eq!((out.value, out.via.as_str()), (PowerState::Off, "b"));
    }
}
