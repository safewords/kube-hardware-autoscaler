//! kube-hardware-autoscaler: Kubernetes Hardware Autoscaler.
//!
//! Declares out-of-band management interfaces (IPMI, Redfish, PiKVM,
//! Wake-on-LAN) for cluster nodes and powers the machines on and off based on
//! scheduling demand.

pub mod controller;
pub mod crd;
pub mod drivers;
pub mod identity;
pub mod membership;
pub mod metrics;
pub mod resources;
pub mod scaling;
pub mod wake;
