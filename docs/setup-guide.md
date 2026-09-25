# kube-hardware-autoscaler setup guide

This guide takes you from an existing cluster with bare-metal nodes to a pool
of machines that power on when pods can't be scheduled and power off when
they sit idle.

- [How it works](#how-it-works)
- [1. Prerequisites](#1-prerequisites)
- [2. Install the operator](#2-install-the-operator)
- [3. Store management credentials](#3-store-management-credentials)
- [4. Describe your machines](#4-describe-your-machines)
  - [Multiple interfaces and fallback](#multiple-interfaces-and-fallback)
- [5. Verify each machine](#5-verify-each-machine)
- [6. Create a pool and turn on autoscaling](#6-create-a-pool-and-turn-on-autoscaling)
- [7. Operate](#7-operate)
- [Driver reference](#driver-reference)
- [Scaling reference](#scaling-reference)
- [Adding a driver](#adding-a-driver)
- [Troubleshooting](#troubleshooting)

## How it works

The operator has two custom resources (cluster-scoped, group `hardware-autoscaler.safewords.com/v1alpha1`):

| Resource | Purpose |
|---|---|
| `NodePowerManagementConfig` | One per machine, named exactly like its `Node`. Binds that Node to an ordered list of management interfaces (driver + config + credentials each) and carries it through its power lifecycle. |
| `NodeScalingPool` | Selects a class of machines **by Node labels** (`spec.nodeSelector`) and decides from cluster demand which of them should be on or off. |

**Which machines a pool controls** is explicit: a machine is in a pool when it has a
`NodePowerManagementConfig` **and** its Node's labels match the pool's `nodeSelector`. The
selector is required and can't be empty. A Node matched by two pools is a conflict and
belongs to neither. Label a node to enroll it, and remove the label to take it out.

The **NodeScalingPool controller** runs every 15 seconds. It:

- **scales up** when pods are `Unschedulable` for longer than `pendingPodGraceSeconds`. It
  bin-packs them onto powered-off members, checking resources, `nodeSelector`, required node
  affinity and taints, and powers on only the machines needed. Capacity that is already
  booting is counted first.
- **scales down** a member when its CPU and memory requests both stay under
  `utilizationThresholdPercent` for `unneededSeconds`. Its pods must also fit on the remaining
  online members. No pods may be pending, and `minOnline` must still be met.
- enforces `minOnline` / `maxOnline`.

It records each decision in the member's `status.scalingDecision`. The
**NodePowerManagementConfig controller** carries it out:

```
Off ──power on──▶ PoweringOn ──node Ready──▶ On ──cordon──▶ Draining ──pods evicted──▶ PoweringOff ──▶ Off
                                               (uncordon)               (graceful, forced after timeout)
```

Nodes keep their `Node` objects while powered off. The operator only
uncordons nodes that it cordoned itself.

### Safety gates

Before any power action, a machine must pass four checks. Each is reported as a condition
on the `NodePowerManagementConfig`. If a check fails, the operator only observes the machine
and takes no action.

| Condition | Guards against |
|---|---|
| `NodeFound` | A typo in `spec.nodeName`. Without a live Node the machine can't be drained, so it's never touched. |
| `PoolMembership` | A machine in no pool, or in two (`Conflict`). Decisions only count while the machine is still in the pool that made them. Deleting a pool, or relabelling a Node, drops its old decisions instead of leaving a stale "off". |
| `IdentityVerified` | An interface pointing at the **wrong machine**. Redfish (`UUID`) and IPMI (Get System GUID) report a hardware ID that's compared with the Node's SMBIOS `systemUUID`. On a mismatch, that interface is disabled for every operation. Drivers that can't report an ID show `Unknown`. |
| `PowerStateConsistent` | The same mistake, for drivers without an ID: an interface reporting **Off** while the Node is Ready and its kubelet lease was renewed seconds ago can't be controlling this machine. All power actions are refused until it's resolved. |

The API server also rejects a `NodePowerManagementConfig` whose `metadata.name` differs from
`spec.nodeName`, so two configs can never claim one machine. It also rejects a
`NodeScalingPool` with an empty `nodeSelector`, so a pool can't accidentally select every node.

## 1. Prerequisites

- Kubernetes 1.26+ and Helm 3.
- Each machine registers as a Kubernetes node and rejoins the cluster
  automatically on boot (kubelet enabled as a service).
- Each machine has **one** reachable management interface:
  - **IPMI**: IPMI v2.0 / RMCP+ with cipher suite 3, UDP 623 reachable from the operator pod,
    and a user with at least OPERATOR privilege.
  - **Redfish**: HTTPS to the BMC and a user allowed to run `ComputerSystem.Reset`.
  - **PiKVM**: HTTPS to the KVM, with the ATX board wired to the machine.
  - **JetKVM**: the ATX or DC extension, and JetKVM connected to an MQTT broker the operator can reach.
  - **Wake-on-LAN**: WoL enabled in BIOS/NIC, and the operator running with
    `hostNetwork: true` on the same L2 segment.
- BIOS power-restore policy should be *stay off* (or *last state*), so that a
  power blip doesn't bring machines up behind the operator's back.
- The operator itself must not run on a machine it may power off. Pin it to the
  control plane or to nodes that are in no pool (see `affinity` / `nodeSelector` below).

## 2. Install the operator

Build and push the image (or use a published one):

```sh
docker build -t ghcr.io/<you>/kube-hardware-autoscaler:0.1.0 .
docker push ghcr.io/<you>/kube-hardware-autoscaler:0.1.0
```

Install the chart. The CRDs in `charts/kube-hardware-autoscaler/crds/` are installed automatically:

```sh
helm install kube-hardware-autoscaler charts/kube-hardware-autoscaler \
  --namespace kube-hardware-autoscaler --create-namespace \
  --set image.repository=ghcr.io/<you>/kube-hardware-autoscaler \
  --set image.tag=0.1.0 \
  --set operator.dryRun=true \
  --set-json 'nodeSelector={"node-role.kubernetes.io/control-plane":""}' \
  --set-json 'tolerations=[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}]'
```

`operator.dryRun=true` makes the operator log every power action instead of
executing it. Start this way and switch it off once the decisions look right.

Useful values (see `charts/kube-hardware-autoscaler/values.yaml` for all):

| Value | Default | Notes |
|---|---|---|
| `operator.dryRun` | `false` | Log power actions without executing them. |
| `operator.opTimeoutSeconds` | `60` | Upper bound for one management-interface call. |
| `operator.shutdownImage` | `debian:stable-slim` | Image of Wake-on-LAN shutdown pods (needs `sh` + `nsenter`). |
| `hostNetwork` | `false` | Required for Wake-on-LAN; helps when BMCs are only reachable from the node network. |
| `credentials` | `[]` | Convenience: creates credential Secrets. |
| `nodePools` / `nodePowerManagementConfigs` | `[]` | Convenience: creates the custom resources from values. |
| `metrics.serviceMonitor.enabled` | `false` | Prometheus-operator ServiceMonitor. |

If you use Wake-on-LAN, the operator namespace must allow privileged pods:

```sh
kubectl label namespace kube-hardware-autoscaler pod-security.kubernetes.io/enforce=privileged --overwrite
```

**Upgrading CRDs:** Helm never upgrades files in `crds/`. After upgrading the chart, run
`kubectl apply -f charts/kube-hardware-autoscaler/crds/`.

## 3. Store management credentials

Credentials are always read from Secrets in the **operator's namespace**. A
`NodePowerManagementConfig` cannot point at Secrets anywhere else, so anyone who can create a
`NodePowerManagementConfig` still can't read Secrets from other namespaces through the operator.

```sh
kubectl -n kube-hardware-autoscaler create secret generic bmc-worker-1 \
  --from-literal=username=ADMIN \
  --from-literal=password='s3cret'
```

| Key | Used by | Notes |
|---|---|---|
| `username`, `password` | ipmi, redfish, pikvm, jetkvm (broker login) | Rename with `credentialsSecretRef.usernameKey` / `passwordKey`. |
| `kgKey` | ipmi | Optional BMC key (Kg) for two-key logins. |

Several machines can share one Secret. The operator reads Secrets on every
reconcile, so rotated credentials take effect without a restart.

## 4. Describe your machines

One `NodePowerManagementConfig` per machine. `metadata.name` and `spec.nodeName` must both
equal the Kubernetes node name, and the API server enforces this.

```yaml
apiVersion: hardware-autoscaler.safewords.com/v1alpha1
kind: NodePowerManagementConfig
metadata:
  name: worker-1             # must equal spec.nodeName
spec:
  nodeName: worker-1
  powerPolicy: Auto          # Auto (its pool decides) | AlwaysOn | AlwaysOff (manual override)
  powerInterfaces:
    - driver: ipmi
      credentialsSecretRef:
        name: bmc-worker-1
      config:
        address: 192.168.10.11
  lifecycle:                 # optional, defaults shown
    bootTimeoutSeconds: 900
    drainTimeoutSeconds: 300
    shutdownTimeoutSeconds: 300
    forceAfterDrainTimeout: false
```

More examples: [`examples/`](../examples) (IPMI, Redfish, PiKVM, JetKVM, Wake-on-LAN,
fallback chains, NodeScalingPool).

There's no pool field. A machine joins a pool when its Node's labels match the pool's
`nodeSelector` (step 6). Until then the operator only observes it, so it's safe to apply
these first and check the machines' status.

### Multiple interfaces and fallback

`powerInterfaces` is an ordered list. Each operation (read state, power on,
power off) goes through the entries top to bottom:

1. Entries whose `actions` don't include the operation are skipped
   (`status`, `powerOn`, `powerOff`; all three when `actions` is omitted).
2. Each attempt is limited by the entry's `timeoutSeconds` (default: the operator's
   `opTimeoutSeconds`). A timeout, an error, or an `Unknown` power state moves on to
   the next entry.
3. The first entry that succeeds wins, and the entries after it are not contacted.

An entry that can't be built at all (missing Secret, invalid config) is reported,
but the remaining entries are still used. The machine only goes to `Error` when no
entry is usable.

```yaml
  powerInterfaces:
    - name: bmc                # optional label; defaults to the driver name
      driver: ipmi
      timeoutSeconds: 20
      credentialsSecretRef: { name: bmc-worker-5 }
      config: { address: 192.168.10.15 }
    - name: wol
      driver: wakeOnLan
      actions: [powerOn]       # only used to power on
      config: { macAddress: "aa:bb:cc:dd:ee:05" }
```

To see which interface did the work:

- `status.lastPowerAction.via` (the `VIA` column of `kubectl get nodepowermanagementconfigs`) is the
  interface that carried out the last power action.
- `status.interfaceWarnings` lists interfaces that failed or are misconfigured while a
  fallback succeeded. A broken primary interface stays visible even though everything works.
- Events (`PowerOn`, `PowerOff`, ...) name the interface used and the ones skipped before it.

Some common setups:

| Setup | Why |
|---|---|
| BMC first, then Wake-on-LAN with `actions: [powerOn]` | The machine still boots when the BMC is wedged or unreachable. |
| JetKVM `extension: dc`, then `wakeOnLan` with `actions: [powerOff]` | DC can only cut power, so graceful shutdown goes through the in-band shutdown pod. DC is used for power on and for the forced off after `shutdownTimeoutSeconds`. Keep that timeout short. |
| Redfish first, then IPMI | Both reach the same BMC, but through different stacks and firmware paths. |

Put the interface that gives a reliable **power state** first. The `wakeOnLan` driver
reports state from the node's `Ready` condition, so a node that is powered on but
unhealthy looks `Off`. List it after real BMCs, or exclude it from `status`.

## 5. Verify each machine

```sh
kubectl get nodepowermanagementconfigs
# NAME       POOL   POLICY   VIA    PHASE   POWER
# worker-1          Auto     ipmi   On      On      (not in a pool yet: observed only)
```

`POWER` comes from the management interface. `Unknown` together with
`status.message` means the operator could not reach it or log in:

```sh
kubectl get nodepowermanagementconfig worker-1 -o jsonpath='{.status.message}'
```

Test the credentials and the connection directly from the operator pod:

```sh
kubectl -n kube-hardware-autoscaler exec deploy/kube-hardware-autoscaler -- kube-hardware-autoscaler power worker-1 status
```

This goes through the fallback chain and prints the interface used. Add
`--via <name>` to test one specific interface, e.g. `--via wol`.
`power worker-1 on|off|force-off` executes the action immediately, even in dry-run mode.
Try it on a machine you can spare to check that power control really works.

## 6. Create a pool and turn on autoscaling

A pool selects its machines by Node label. Pick a label that means "this class of machine
may be powered on and off". A dedicated opt-in label makes enrollment a deliberate act:

```sh
kubectl label node worker-1 worker-2 worker-3 worker-4 hardware-autoscaler.safewords.com/pool=workers
```

```yaml
apiVersion: hardware-autoscaler.safewords.com/v1alpha1
kind: NodeScalingPool
metadata:
  name: workers
spec:
  nodeSelector:              # required, non-empty; matchLabels and/or matchExpressions
    matchLabels:
      hardware-autoscaler.safewords.com/pool: workers
  minOnline: 1
  maxOnline: 4
  scaleDown:
    utilizationThresholdPercent: 50
    unneededSeconds: 600
```

An existing hardware-type label works just as well, for example `nodeSelector: {matchLabels:
{example.com/gpu: "true"}}` for a pool of GPU machines. `matchExpressions` supports `In`,
`NotIn`, `Exists` and `DoesNotExist`, for example to exclude nodes in maintenance.

```sh
kubectl apply -f examples/nodescalingpool.yaml
kubectl get nodescalingpools
# NAME      MIN   MAX   ONLINE   TOTAL   PENDING
# workers   1     4     2        4       0
kubectl get nodepowermanagementconfigs
# NAME       POOL      POLICY   VIA    PHASE   POWER
# worker-1   workers   Auto     ipmi   On      On
```

`status.members` and `status.conflicts` on the pool list exactly which machines it controls.
Each machine's `status.pool` and its `PoolMembership` condition show the same from the
machine's side.

Watch the decisions in dry-run mode (`kubectl -n kube-hardware-autoscaler logs deploy/kube-hardware-autoscaler -f`
and `kubectl get events --field-selector involvedObject.kind=NodePowerManagementConfig`).
Then enable real power control:

```sh
helm upgrade kube-hardware-autoscaler charts/kube-hardware-autoscaler -n kube-hardware-autoscaler --reuse-values --set operator.dryRun=false
```

To test scale-up, create more demand than the online machines can serve:

```sh
kubectl create deployment burn --image=registry.k8s.io/pause:3.9 --replicas=20
kubectl set resources deployment burn --requests=cpu=1
kubectl get pods -w          # pods go Pending ...
kubectl get nodepowermanagementconfigs -w  # ... and machines go PoweringOn -> On
kubectl delete deployment burn   # after unneededSeconds, machines drain and power off
```

## 7. Operate

- **Maintenance:** `kubectl patch nodepowermanagementconfig worker-1 --type merge -p '{"spec":{"powerPolicy":"AlwaysOff"}}'`
  drains the machine and powers it off. The autoscaler leaves it alone until you set `Auto` again. (Values are deliberately not `On`/`Off`: YAML 1.1 tooling reads those as booleans.)
- **Keep a machine on:** `powerPolicy: AlwaysOn`.
- **Pods that must not be moved:** annotate them with `hardware-autoscaler.safewords.com/safe-to-evict: "false"`.
  `cluster-autoscaler.kubernetes.io/safe-to-evict` is honoured too. Their node is never scaled down.
  Bare pods (no controller) also block scale-down, unless annotated `safe-to-evict: "true"`.
- **PodDisruptionBudgets** are respected: drains use the eviction API. A drain that
  can't finish within `drainTimeoutSeconds` is aborted and the node uncordoned.
  The pool then leaves that machine alone for `drainFailureBackoffSeconds`.
  Set `forceAfterDrainTimeout: true` to power off anyway.
- **Metrics** on `:8080/metrics`: `kha_power_actions_total`, `kha_node_power_state`,
  `kha_pool_nodes`, `kha_pool_pending_pods`, `kha_reconcile_errors_total`.

## Driver reference

List the drivers and their config JSON schemas with `kube-hardware-autoscaler drivers --schemas`.
Unknown `config` fields are rejected, and the error appears in `status.message`.

### `ipmi`

IPMI v2.0 over LAN (RMCP+). The operator speaks the protocol itself, so no
`ipmitool` is needed. Authentication is RAKP-HMAC-SHA1, with HMAC-SHA1-96 integrity
and AES-CBC-128 confidentiality (cipher suite 3). Each operation opens a session
and closes it again.

| Field | Default | |
|---|---|---|
| `address` | required | BMC host or IP |
| `port` | `623` | |
| `privilegeLevel` | `ADMINISTRATOR` | `OPERATOR` is enough for power control |
| `requestTimeoutMs` | `2000` | per UDP request |
| `attempts` | `3` | sends per request |

Power off is an ACPI soft shutdown (chassis control 5). It becomes a hard power-down
(chassis control 0) after `shutdownTimeoutSeconds`.

Limitations: IPMI v1.5 (`lan`) and SHA-256 cipher suites (15-17) are not supported.
Use Redfish on BMCs that only allow those.

### `redfish`

| Field | Default | |
|---|---|---|
| `endpoint` | required | e.g. `https://10.0.0.10` |
| `systemId` | first system | ComputerSystem id |
| `insecureSkipVerify` | `false` | accept self-signed BMC certificates |

The driver uses HTTP basic auth and `ComputerSystem.Reset` with `On`,
`GracefulShutdown`, and `ForceOff` after the shutdown timeout.

### `pikvm`

| Field | Default | |
|---|---|---|
| `endpoint` | required | e.g. `https://pikvm.lan` |
| `insecureSkipVerify` | `false` | |

The driver uses `/api/atx` for the power LED state and `/api/atx/power?action=on|off|off_hard`.

### `nanokvm`

Sipeed NanoKVM with the ATX board, through its web API.

| Field | Default | |
|---|---|---|
| `endpoint` | required | e.g. `http://192.168.1.50` |
| `insecureSkipVerify` | `false` | when TLS is enabled on the NanoKVM |
| `shortPressMs` | `800` | power on / graceful off |
| `longPressMs` | `6000` | forced off (ATX needs more than 4 s) |

`credentialsSecretRef` holds the NanoKVM web login. The driver logs in the way the NanoKVM
web UI does, reuses the session cookie, and logs in again when it expires. The power state
comes from the power-LED input (`GET /api/vm/gpio`), so wire the motherboard's power-LED
header to the NanoKVM, or the machine always reads as off. The button toggles, so the driver
only presses it when the machine isn't already in the requested state.

### `jetkvm`

JetKVM has no HTTP API for power control; its own RPC runs over a WebRTC data
channel. The driver uses JetKVM's built-in **MQTT integration** instead. On the JetKVM:

1. Settings → MQTT: enable it, point it at your broker, and turn on **Enable actions**.
2. Activate the **ATX** or **DC** power extension.
3. Note the base topic. JetKVM appends its device ID, e.g. `jetkvm/abc123`. You can find it
   with `mosquitto_sub -t 'jetkvm/#' -v`.

| Field | Default | |
|---|---|---|
| `broker` | required | `mqtt://host:1883` or `mqtts://host:8883` (system CA roots) |
| `baseTopic` | required | including the device ID, e.g. `jetkvm/abc123` |
| `extension` | `atx` | `atx` or `dc` |
| `stateTimeoutMs` | `5000` | wait for the retained state after subscribing |

`credentialsSecretRef` is optional. When set, it holds the **broker** username and password.

- **State** is read from the retained `{base}/atx/state` (power LED) or `{base}/dc/state`.
  If `{base}/status` reports `online: false`, the attempt fails and the next interface is tried.
- **ATX:** power on and graceful off send a short press (`atx_power_short`). A forced off
  sends a 5-second press (`atx_power_long`). The button toggles, so the driver reads the state
  first and never presses it when the machine is already in the requested state.
- **DC:** power on and forced off send `dc_power` `ON`/`OFF`. A graceful off is refused, so
  pair it with an in-band shutdown interface (see [fallback](#multiple-interfaces-and-fallback)).

### `wakeOnLan`

No credentials are needed.

| Field | Default | |
|---|---|---|
| `macAddress` | required | `aa:bb:cc:dd:ee:ff` |
| `broadcastAddress` | `255.255.255.255` | use the subnet broadcast when routing is involved |
| `port` | `9` | |
| `shutdownImage` | operator default | needs `sh` + `nsenter` |

Power on sends a magic packet. Power off runs a privileged, `hostPID` pod on the node
that calls `systemctl poweroff` in the host namespaces. That pod stores the node's boot
ID and does nothing if the host has rebooted since, so a leftover pod can never shut down
a freshly booted machine. The power state is inferred from the node's `Ready` condition,
which lags a real shutdown by the kubelet grace period (about 40 s). Add a `ping` interface
in front for an accurate reading.

### `ping`

Status only: reports **On** when the machine answers on the network and **Off** when it
doesn't. It never powers anything on or off; the chain skips it for those operations
automatically. Put it first in `powerInterfaces` to give drivers without a real power reading
(Wake-on-LAN, or a KVM without the power-LED header wired) an accurate one:

```yaml
powerInterfaces:
  - driver: ping
    actions: [status]
    config:
      method: icmp              # or tcp (default); address defaults to the Node's IP
  - driver: wakeOnLan
    config: { macAddress: "aa:bb:cc:dd:ee:07" }
```

| Field | Default | |
|---|---|---|
| `address` | the Node's `InternalIP` | host name or IP of the machine; IPv4 is preferred. Only needed to probe a different address. |
| `method` | `tcp` | `tcp` or `icmp` (IPv4 only) |
| `port` | `22` | TCP port for `tcp` |
| `timeoutMs` | `1000` | per probe |
| `attempts` | `3` | probes before declaring the machine off |

- **`tcp`:** connects to `port`. Both an accepted and a *refused* connection prove the host is
  up. Only a timeout or "unreachable" counts as off. Needs no privileges.
- **`icmp`:** ICMP echo through an unprivileged ping socket. Linux allows these when
  `net.ipv4.ping_group_range` includes the operator's group (gid 65532). Many distributions
  allow every group by default; check with `cat /proc/sys/net/ipv4/ping_group_range`. With
  `hostNetwork: true` the host's setting applies. Otherwise set the sysctl on the pod through
  `podSecurityContext.sysctls` in the chart values.
- **No definite answer:** if every probe fails for another reason (for example, no route on the
  operator's side), the reading is `Unknown`, and the chain falls through to the next interface.

## Scaling reference

`NodeScalingPool.spec` (defaults shown; `nodeSelector` is required):

```yaml
nodeSelector:                 # required, non-empty
  matchLabels: {}             # all must match
  matchExpressions: []        # In, NotIn, Exists, DoesNotExist
minOnline: 0
maxOnline: null               # unlimited
scaleUp:
  enabled: true
  pendingPodGraceSeconds: 30
  maxNodesPerStep: 3
scaleDown:
  enabled: true
  utilizationThresholdPercent: 50
  unneededSeconds: 600
  delayAfterScaleUpSeconds: 600
  drainFailureBackoffSeconds: 1800
  maxNodesPerStep: 1          # concurrent drains/power-offs
```

The scheduling model covers resource requests (CPU, memory, pod count),
`nodeSelector`, required node affinity, and taints and tolerations. It ignores
the taints a node gets from being off or cordoned. Pod (anti-)affinity and topology
spread constraints are not simulated. The worst case is one extra machine powered
on for a pod that still doesn't fit. That machine is reclaimed by scale-down.

Labels and allocatable resources are taken from the `Node` object. They are also
cached in `NodePowerManagementConfig.status`, so they stay available when a node object is
missing while the machine is off.

## Adding a driver

Each driver is one file in `src/drivers/` implementing two traits:

```rust
pub struct MyDriver { /* ... */ }

#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MyConfig { pub endpoint: String }

impl DriverKind for MyDriver {
    const NAME: &'static str = "mydriver";          // spec.powerInterfaces[].driver
    const DESCRIPTION: &'static str = "My management interface";
    const REQUIRES_CREDENTIALS: bool = true;         // default
    type Config = MyConfig;                          // spec.powerInterfaces[].config
    fn build(config: MyConfig, init: &DriverInit<'_>) -> Result<Self> { /* init.credentials()?, init.ctx */ }
}

#[async_trait]
impl PowerDriver for MyDriver {
    async fn power_state(&self) -> Result<PowerState>;
    async fn power_on(&self) -> Result<()>;
    async fn power_off(&self, force: bool) -> Result<()>;
}
```

Then add `&Entry::<MyDriver>(PhantomData)` to `CATALOG` in `src/drivers/mod.rs`.
Config validation, the `drivers --schemas` output, the `power` CLI and the controllers
pick it up automatically. The CRD does not change.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `PHASE Error`, message `unknown driver` / `config: unknown field` | Typo in `powerInterfaces`; see `kube-hardware-autoscaler drivers --schemas`. |
| message `secret kube-hardware-autoscaler/x not found` | Secrets must be in the operator namespace. |
| `POWER Unknown`, `ipmi: timeout` | UDP 623 blocked, wrong address, or IPMI-over-LAN disabled in the BMC. Try `hostNetwork: true`. |
| `ipmi: authentication failed` | Wrong user/password/Kg, or the BMC does not allow cipher suite 3. |
| `BootTimeout` event | The machine powered on but the node never became Ready: check BIOS boot order and kubelet. The operator retries. |
| `DrainFailed` event | A PDB or blocking pod prevented eviction; see `status.message` for the pods. |
| Machines never scale down | `kubectl get nodescalingpool -o yaml`: `status.message` explains holds (pods pending, recently scaled up, ...). Nodes with bare or `safe-to-evict=false` pods are never candidates. |
| Nothing happens at all | Check `operator.dryRun`, `powerPolicy: Auto`, and that the Node carries the pool's labels (`kubectl get npmc` shows the POOL; check the `PoolMembership` condition). |
| `status.interfaceWarnings` is not empty | A higher-priority interface failed and a fallback took over. Fix the listed interface. |
| JetKVM: `no retained state on .../atx/state` | MQTT disabled on the JetKVM, wrong `baseTopic` (it must include the device ID), or the ATX/DC extension is not active. |
| JetKVM: commands have no effect | "Enable actions" is off in the JetKVM MQTT settings. |
| Condition `NodeFound=False` | `spec.nodeName` doesn't match any Node. Fix the name (it must also equal `metadata.name`). |
| Condition `PoolMembership=False, reason Conflict` | Two pools' `nodeSelector`s both match this Node. Narrow one of them. |
| Condition `IdentityVerified=False` | An interface reports a different system UUID than the Node: it points at another machine. That interface is disabled. Fix its address. |
| Condition `PowerStateConsistent=False` | An interface reports Off although the Node is alive, so it's probably wired to another machine. No power actions are taken until it agrees. |
