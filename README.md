# kube-hardware-autoscaler

[![ci](https://github.com/safewords/kube-hardware-autoscaler/actions/workflows/ci.yml/badge.svg)](https://github.com/safewords/kube-hardware-autoscaler/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**A Kubernetes operator that powers bare-metal nodes on and off based on demand.**

Describe how each machine can be powered (IPMI, Redfish, PiKVM, NanoKVM, JetKVM or
Wake-on-LAN, with fallback between them) and which class of machines may be scaled.
kube-hardware-autoscaler powers machines on when pods can't be scheduled, and drains
and powers them off when they sit idle. It's the cluster-autoscaler idea for hardware
you own.

```yaml
# The machine: how to power it. Named exactly like its Node.
apiVersion: hardware-autoscaler.safewords.com/v1alpha1
kind: NodePowerManagementConfig
metadata:
  name: worker-1
spec:
  nodeName: worker-1
  powerInterfaces:              # tried in order; first success wins
    - driver: ipmi
      credentialsSecretRef: { name: bmc-worker-1 }
      config: { address: 192.168.10.11 }
    - driver: wakeOnLan         # fallback for powering on
      actions: [powerOn]
      config: { macAddress: "aa:bb:cc:dd:ee:01" }
---
# The policy: which class of machines, and when to scale it.
apiVersion: hardware-autoscaler.safewords.com/v1alpha1
kind: NodeScalingPool
metadata:
  name: workers
spec:
  nodeSelector:
    matchLabels: { hardware-autoscaler.safewords.com/pool: workers }
  minOnline: 1
  maxOnline: 4
```

## Features

- **Driver catalog:** `ipmi` (native IPMI v2.0/RMCP+, no `ipmitool`), `redfish`, `pikvm`,
  `nanokvm`, `jetkvm` (via its MQTT integration), `wakeOnLan` (with an in-band shutdown pod, and
  relays on neighbouring nodes for machines on other network segments or sites
  such as VPN-linked locations) and `ping` (status only, via TCP or ICMP). Adding a driver takes one file and one
  catalog line, and the CRD doesn't change.
- **Several interfaces per machine:** tried in priority order, with per-interface timeouts
  and per-operation `actions`. Degraded interfaces show up in status.
- **Explicit membership:** pools select machines by Node label. A machine matched by two
  pools belongs to neither, and deleting a pool drops its decisions.
- **Safety gates before every power action:**
  - The Node must exist.
  - BMCs must report the same system UUID as the Node, or that interface is disabled.
  - An interface reporting Off for a live Node blocks all actions.
  - One config per Node, enforced by the API server.
- **Shutdown or standby:** idle machines are shut down, or with `powerOffMode: Standby`
  suspended to RAM and woken in seconds.
- **Demand-driven scaling:** unschedulable pods are bin-packed onto powered-off machines,
  checking resources, node selectors, node affinity and taints. Idle machines are drained
  through the eviction API, which respects PDBs. `safe-to-evict` annotations are honoured.
- **Operations:** dry-run mode, Kubernetes events, Prometheus metrics, and a `power` CLI to
  test each interface.

## Install

The chart and the image are published to GHCR:

```sh
helm install kube-hardware-autoscaler oci://ghcr.io/safewords/charts/kube-hardware-autoscaler \
  --namespace kube-hardware-autoscaler --create-namespace \
  --set operator.dryRun=true
```

The image is `ghcr.io/safewords/kube-hardware-autoscaler`. Start in dry-run mode, where power
actions are only logged, and switch it off once the decisions look right.

The **[setup guide](docs/setup-guide.md)** covers:
- prerequisites and credentials;
- describing machines and verifying their interfaces;
- creating pools;
- the driver and scaling reference;
- troubleshooting.

## CLI

```
kube-hardware-autoscaler run                               # the operator
kube-hardware-autoscaler crds                              # print the CRDs
kube-hardware-autoscaler drivers [--schemas]               # list drivers and their config schemas
kube-hardware-autoscaler power <node> status|on|off|force-off [--via <interface>]
```

## Development

```sh
cargo test                  # unit tests + example manifests validated against the CRDs
cargo clippy --all-targets
sh hack/gen-crds.sh         # regenerate chart CRDs after changing src/crd.rs
helm lint charts/kube-hardware-autoscaler
sh hack/e2e/run.sh          # optional, manual: end-to-end on a local 2-node minikube
```

The optional end-to-end test isn't part of CI. Run it locally when changing the CRDs, RBAC,
the chart or the controllers. It runs the real image and chart against a real API server,
with a fake JetKVM speaking JetKVM's MQTT protocol in place of real hardware. It covers:
- scale-down and scale-up through the fallback chain;
- the CEL admission rules;
- label-based membership and pool conflicts;
- the safety gates: a wrong-machine interface and a missing Node are never acted on.

## License

[Apache-2.0](LICENSE)
