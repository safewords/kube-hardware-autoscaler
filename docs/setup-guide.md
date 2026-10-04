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
selector is required and can't be empty. Label a node to enroll it, and remove the label
to take it out. Pools may overlap; see [Overlapping pools](#overlapping-pools).

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
| `PoolMembership` | Which pools select the machine (`status.pools`), or none. Decisions only count while the machine is still in the pool that made them. Deleting a pool, or relabelling a Node, drops its old decisions instead of leaving a stale "off". |
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
  - **Wake-on-LAN**: WoL enabled in BIOS/NIC, and either the operator running with
    `hostNetwork: true` on the same L2 segment, or another node on that segment acting as a relay
    (see [relays](#waking-machines-on-other-segments-or-sites-relays)), e.g. across sites linked by a VPN.
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
| `operator.shutdownImage` | `debian:stable-slim` | Image of in-band shutdown and standby pods (needs `sh` + `nsenter`). |
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
    powerOffMode: Shutdown   # Shutdown | Standby | Auto (S3 sleep where available, see below)
```

More examples: [`examples/`](../examples) (IPMI, Redfish, PiKVM, JetKVM, Wake-on-LAN,
fallback chains, NodeScalingPool).

There's no pool field. A machine joins a pool when its Node's labels match the pool's
`nodeSelector` (step 6). Until then the operator only observes it, so it's safe to apply
these first and check the machines' status.

### Standby instead of shutdown

With `lifecycle.powerOffMode: Standby`, an idle machine is suspended to RAM (ACPI S3)
instead of shut down. It draws a few watts and is back in seconds rather than after a
full boot, with its page cache and container images still in memory.

```yaml
spec:
  powerInterfaces:
    - driver: ping                        # accurate state: a suspended machine stops answering
      config: { method: icmp }
    - driver: wakeOnLan                   # wakes the machine from S3
      config: { macAddress: "aa:bb:cc:dd:ee:ff" }
  lifecycle:
    powerOffMode: Auto                    # or Standby
```

`Auto` is the safe choice for a mixed fleet. It checks each machine and only sleeps the ones
that can do so properly:

- Once per boot, while the machine is online, a small unprivileged pod reads
  `/sys/power/mem_sleep` through a read-only mount. If the kernel offers `deep` (ACPI S3),
  idle periods suspend the machine in S3; `deep` is selected explicitly, so a kernel whose
  default is `s2idle` still sleeps in S3. Without `deep` (only `s2idle`, which many NICs
  can't wake from), the machine is shut down. The result is in `status.sleepSupport`, and a
  `SleepProbe` event records it.
- If the probe hasn't finished 60 s after the drain, the machine is shut down.
- If sleeping fails (see below), the machine is shut down for the next 24 hours, as with
  `Standby`.

`Standby` always suspends, using the kernel's default sleep mode, until it fails.

In a pool that mixes machines with and without S3, set `scaleUp.preferS3Capable: true` on
the `NodeScalingPool` so scale-up picks the S3-capable ones first. After the next idle period
they're back in seconds instead of after a full boot. A machine counts as S3-capable while
it's in `Standby`, or when its last sleep probe found S3 and standby hasn't failed on it in
the past day. A pod that only fits a machine without S3 still gets that machine. The option
is off by default, so scale-up picks the largest machine first. `scaleUp.preferredNodes`
weights rank before it and machine size after it; see
[Preferred machines](#preferred-machines).

- **Entering standby** is always in-band: after the drain, the operator runs a privileged
  pod on the node that calls `systemctl suspend` (the same kind of pod the `wakeOnLan`
  driver uses to shut down, using `operator.shutdownImage`). BMCs and KVMs can't suspend
  a machine, so no interface is needed for it.
- **Waking** goes through the `powerOn` interfaces as usual. Wake-on-LAN is the most reliable
  way to wake from S3; enable it in the firmware and the OS (`ethtool -s <nic> wol g`). A KVM
  power-button press usually works too. IPMI and Redfish `power on` often do nothing to a
  suspended machine, since the BMC considers it already on.
- **Detection:** the machine counts as in standby (phase `Standby`) once its power reads Off
  or its Node stops being Ready. Some BMCs report a suspended machine as On, so the Node
  going NotReady is enough. While waking, the power-on request is repeated until the Node
  is Ready, whatever the interface reports.
- If demand returns before the machine has gone to sleep, the suspend pod is withdrawn and
  the machine is woken.
- **If standby fails**, the operator emits a `StandbyFailed` warning event, sets
  `status.standbyFailedAt`, and shuts the machine down instead of suspending it for the
  next 24 hours. Standby counts as failed when:
  - the machine is still up 240 s after the suspend (or `shutdownTimeoutSeconds`, if
    shorter). It is then shut down gracefully;
  - it wakes up from standby by itself. It is drained again and shut down, rather than
    suspended in a loop;
  - it does not wake within `bootTimeoutSeconds`. It is then forced off so the next power
    on is a cold boot. This needs an interface that can force it off while it's asleep,
    such as a BMC whose user is allowed to control power. The in-band shutdown of
    `wakeOnLan` can't reach a sleeping machine.

Check that `systemctl suspend` works on the machine, and that it wakes up from the chosen
interface, before enabling this. A machine that wakes up the moment it is suspended usually
has a PCIe port or USB controller allowed to wake it. `/proc/acpi/wakeup` lists them, and
writing an entry's name to that file toggles it. Leave the entry above the NIC enabled.

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

`status.members` on the pool lists exactly which machines it controls. Each machine's
`status.pools` (and `status.pool`, comma-separated, in the POOL column) and its
`PoolMembership` condition show the same from the machine's side.

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
| `broadcastAddress` | automatic | when omitted, each sender uses every interface's broadcast address plus `255.255.255.255` |
| `port` | `9` | |
| `sendFromOperator` | `true` | send from the operator's own node |
| `relay` | none | also send from neighbouring nodes (see below) |
| `shutdownImage` | operator default | needs `sh` + `nsenter` |

Power on sends a magic packet. Power off runs a privileged, `hostPID` pod on the node
that calls `systemctl poweroff` in the host namespaces. That pod stores the node's boot
ID and does nothing if the host has rebooted since, so a leftover pod can never shut down
a freshly booted machine. The power state is inferred from the node's `Ready` condition,
which lags a real shutdown by the kubelet grace period (about 40 s). Add a `ping` interface
in front for an accurate reading.

#### Waking machines on other segments or sites (relays)

A magic packet is a broadcast. It doesn't cross routers or VPN tunnels, so the operator can
only wake machines on its own node's network segment. With `relay`, the operator starts a
short-lived pod on one or more **neighbouring nodes**, and each one sends the packet on
every network interface of its node. A machine at another site is woken by nodes that share
its network.

```yaml
- driver: wakeOnLan
  config:
    macAddress: "aa:bb:cc:dd:ee:07"
    sendFromOperator: false            # the operator runs at another site
    relay:
      sameTopologyAs: topology.kubernetes.io/zone   # nodes at the same site
      nodeSelector: {}                 # optionally restrict which nodes may relay
      maxNodes: 3                      # default
```

- **Candidate relays** are Ready nodes other than the target that the operator hasn't powered
  off.
- **`sameTopologyAs`** keeps only candidates whose value for that label equals the target
  Node's value. The target's Node object keeps its labels while the machine is off. Label
  each node with its site (the standard `topology.kubernetes.io/zone` works well). If the
  target lacks the label, no relay is chosen.
- **Without `sameTopologyAs`**, the packet fans out from up to `maxNodes` nodes anywhere in the
  cluster. That's topology-agnostic: whichever relay shares the target's network delivers it.
- **How relays run:** they talk only to the Kubernetes API, with no listening ports or extra
  daemons. Each is a pod using the operator's own image (`kube-hardware-autoscaler wake`),
  with `hostNetwork`, non-root, and all capabilities dropped. The pods finish within seconds,
  and the next wake replaces them.
- **Mixing both:** with `sendFromOperator: true` (the default) *and* `relay`, the packet
  goes out both ways. The wake succeeds if either path sends it.

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
  requireExplicitSelection: false   # see "Dedicated pools"
  preferS3Capable: false            # power on S3-capable machines first (see "Standby")
  preferredNodes: []                # [{name: <Node>, weight: 0-100}]; see "Preferred machines"
scaleDown:
  enabled: true
  utilizationThresholdPercent: 50
  unneededSeconds: 600
  holdAfterPowerOnSeconds: 600      # see "Holding after a power-on"
  drainFailureBackoffSeconds: 1800
  maxNodesPerStep: 1          # concurrent drains/power-offs
  ignoreDaemonSetUtilization: false
  ignoreNonSelectingPodUtilization: false
```

### Overlapping pools

A machine whose Node matches several pools' `nodeSelector`s is a member of each. It still has
exactly one `NodePowerManagementConfig`, named after its Node, so one object holds its power
state whichever pool acts on it. Each pool keeps its own rules:

- **Power-on:** any pool can power the machine on, for its own pending pods. Which pods
  count is decided by the pool's own `requireExplicitSelection`, and the pool's own
  `preferredNodes` ranks the candidates. The pool that woke the machine is recorded as the
  machine's `status.scalingDecision.pool`. Other pools show it as `powering on (woken by
  pool ...)`.
- **`maxOnline`:** each pool counts every member that is on or booting, whichever pool woke
  it.
- **Power-off:** each pool judges the machine by its own rules (`utilizationThresholdPercent`,
  `ignore*Utilization`, `unneededSeconds`, its pending pods, `holdAfterPowerOnSeconds`,
  `minOnline`). The machine is powered off only when every pool selecting it agrees. Each
  pool publishes its side:
  - `status.releasable`: the members it would let go now.
  - `status.needed`: why it keeps each of the others, for example `busy: utilization 40% >=
    10%`, `2 pending pod(s)`, `holding after power-on until 13:12:56Z` or `unneeded since
    13:04:57Z, eligible at 13:09:57Z`.

  A pool powers a machine off only if every other pool selecting it lists it as
  releasable. Otherwise the decision says `kept: needed by pool ci: busy: utilization 50% >=
  10%`. A pool that hasn't evaluated the machine yet keeps it on.

For example, a `gpu` pool (only pods selecting `gpu=true` count) and a `ci` pool (only pods
selecting `ci=true` count) can share a machine. A CI job running on it keeps it on through
`ci`, although `gpu` ignores that job.

### Preferred machines

`scaleUp.preferredNodes` says which machines a pool would rather use, by Node name, with a
weight from 0 to 100. Machines not listed weigh 0.

```yaml
scaleUp:
  preferS3Capable: true
  preferredNodes:
    - name: devbox          # cheap to run, wakes from S3
      weight: 100
    - name: vulpes-zerda    # power-hungry server
      weight: 10
```

Machines are ranked in this order:

1. **Weight**, highest first.
2. **S3 capability**, S3-capable first, only with `preferS3Capable: true`.
3. **Size** (CPU, then memory), largest first. Fewer large machines absorb the pending
   pods.
4. **Node name**, ascending. Ranks are never tied, so the choice never depends on the
   order the API server lists machines in.

- **Scale-up:** each pending pod gets the highest-ranked machine that is off, `Auto`, and
  that it fits on. `minOnline` takes the highest-ranked machines that are off. A preferred
  machine the pod doesn't fit on is skipped.
- **Scale-down:** when several machines are unneeded, they're powered off in exactly the
  reverse order: lowest weight first, then (with `preferS3Capable`) machines without S3,
  then the smallest, then the name descending. The smallest goes first because it takes the
  least capacity away, and the larger machines left can take its pods.

A weight only orders machines that are already candidates. It never makes a machine eligible
that otherwise isn't, and it never keeps a machine on that is otherwise unneeded.

Weight comes before S3 capability because it is the explicit choice for a named machine.
`preferS3Capable` is a general rule about a class of machines. Where both should count, give
the S3-capable machines the higher weight.

A pod's own `preferredDuringSchedulingIgnoredDuringExecution` node affinity is not
considered. Each name may appear once (the API server enforces it, and server-side apply
merges entries by name). Names are checked:

- **When the pool is saved,** the operator's [validating webhook](#validating-webhook)
  refuses a name that matches no Node. It admits a name whose Node the pool's
  `nodeSelector` doesn't select, with a warning.
- **While the pool runs,** the `PreferredNodesValid` condition in `status.conditions` turns
  `False` (reason `UnknownNodes` or `NotInPool`) and lists such names, for example after a
  Node is deleted. The operator logs a warning once. Those entries are ignored, and the
  others still apply.

### Devices that register after boot

A device plugin (for example Intel's GPU plugin, `gpu.intel.com/i915`) reports its devices
only some time after the Node is Ready. Until then the Node's allocatable count for them
is 0, and the machine can look idle while the pods it was woken for are still pending.

The operator remembers each machine's last non-zero extended resources in
`NodePowerManagementConfig.status.extendedResources`. Within `bootTimeoutSeconds` of the
Node becoming Ready, a machine that still reports 0 of such a resource counts as
**needed** while a pending pod of its pool would fit it once that resource registers. It is
not counted as unneeded, so neither `unneededSeconds` nor `holdAfterPowerOnSeconds` is what
keeps it on. The decision log says so: `vulpes-zerda needed: 1 pending pod(s) waiting for its
gpu.intel.com/i915 to register`. A pending pod "of its pool" means one that targets the pool
when `requireExplicitSelection` is set. A pod asking for more devices than the machine ever
had doesn't count.

### Holding after a power-on

`scaleDown.holdAfterPowerOnSeconds` (default 600): after this pool powers any machine on,
for pending pods or for `minOnline`, no machine in the pool is powered off for this long,
however idle it looks. It holds scale-down for the whole pool and never delays a scale-up.
It covers the time between a machine booting and its pods being scheduled and started
(image pulls, runner registration), when the machine looks idle but work is on its way. It
is the equivalent of cluster-autoscaler's `--scale-down-delay-after-add`. While it holds,
`status.message` says `scale down held: holding after power-on`.

The field used to be called `delayAfterScaleUpSeconds`. The old name is still accepted with
the same meaning, but it's deprecated:

- The old name stays in the CRD schema, so existing objects still validate.
- When both names are set, `holdAfterPowerOnSeconds` wins, and the operator logs a warning.
- The webhook warns on save whenever the old name is used.

Neither name has a schema default; the operator applies 600 when both are unset. A
schema default for the new name would be filled in by the API server, and would then
override a value set under the old one.

### Scaling decisions

Every pool decision explains itself: what was chosen, its rank, and the rule that decided
between it and the next candidate (`weight`, `S3 capability`, `size` or `name`). When the pool
does nothing, the explanation says why: pods waiting for `maxOnline`, a scale-down held by
`holdAfterPowerOnSeconds`, or machines not yet unneeded for `unneededSeconds`. The
explanation is published in several places:

- **Events:** `ScaleUpSelected` and `ScaleDownSelected` on the pool and on the machine.
  `ScaleUpBlocked` and `ScaleDownBlocked` on the pool, each time the reason for doing nothing
  changes.
- **`status.recentDecisions`:** the last 20 decisions, kept after the Events expire (about an
  hour). Each records the time, the action (`PowerOn`, `PowerOff` or `NoAction`), the node,
  the reason, and the ranked candidates in compact form:

  ```yaml
  - action: PowerOn
    node: devbox
    reason: "unschedulable pod ci/runner-x; #1 of 2 to power on, over vulpes-zerda by weight (100 vs 10)"
    candidates: "devbox(w100 s3 16c/62Gi) > vulpes-zerda(w10 40c/125Gi)"
    time: "2026-10-04T12:09:10Z"
  ```

  A `NoAction` entry is added only when the reason changes, and it uses absolute times
  ("holding after power-on until 12:29:10Z"), so the status doesn't change on every cycle.
- **Logs:** one info line per decision, with the seconds elapsed and remaining. The
  per-machine ranking is logged at debug.

`--decision-verbosity` (chart value `operator.decisionVerbosity`) sets how much is logged:

| Value | Logs and Events |
|---|---|
| `quiet` | Power actions only. A change in why nothing is done is logged at debug, with no Event. |
| `summary` (default) | Every decision at info, with an Event. |
| `detailed` | Same as `summary`, plus each machine's rank and status at info. |

### Validating webhook

With `--webhook-addr` set (chart: `webhook.enabled`, default on), the operator serves a
validating webhook for `NodeScalingPool` CREATE and UPDATE requests (`failurePolicy: Fail`,
5 s timeout):

- **Refused:** a `scaleUp.preferredNodes` name that matches no Node. On UPDATE, this applies
  only to names being added, so a Node deleted later doesn't block other edits; the edit is
  admitted with a warning instead.
- **Admitted with a warning:** a Node that the pool's `nodeSelector` doesn't select, or use of
  the deprecated `delayAfterScaleUpSeconds`.

Duplicate names and the weight range are enforced by the CRD schema.

The webhook needs no cert-manager. The operator manages it in four steps:

1. **Certificate:** at startup, it issues a self-signed CA and a serving certificate for
   `<service>.<namespace>.svc`, valid for five years and renewed at startup in the last 30
   days. Both are stored in the Secret `<release>-webhook-tls`.
2. **Registration:** once it's listening, it applies the `ValidatingWebhookConfiguration`
   with the CA bundle. The first install therefore never waits on a webhook that isn't
   there yet.
3. **Removal:** on SIGTERM, it deletes that configuration, so an operator rollout doesn't
   refuse pool changes while no pod serves.
4. **Uninstall:** the configuration is owned by the operator's ClusterRole, so uninstalling
   the chart removes it.

With `hostNetwork: true`, `webhook.port` (default 9443) is a port on the node the operator
runs on, so it must be free there.

A crashed operator leaves the configuration in place. Pool changes are then refused until
the operator is back, which is what `failurePolicy: Fail` is for. To change a pool while the
operator is down, delete the `ValidatingWebhookConfiguration` by hand.

### Dedicated pools (standby GPUs and similar)

For a pool that should be on **only while its own work exists**, for example a GPU machine
woken for transcode Jobs, set these three options:

```yaml
scaleUp:
  requireExplicitSelection: true           # only pods that target the pool can wake it
scaleDown:
  ignoreDaemonSetUtilization: true         # DaemonSets don't keep it on
  ignoreNonSelectingPodUtilization: true   # guests (e.g. CI runners) don't keep it on
```

- **Targeting the pool:** a pod targets it when its `nodeSelector`, or every required
  node-affinity term (`In` with exactly that value), requires all of the pool's
  `nodeSelector.matchLabels`. Pods without such a selector never power the pool on, even when
  they're Unschedulable and would fit.
- **What counts as load:** utilization then counts only the pods that target the pool. Guest
  pods are still drained before power-off, and they must fit on the remaining online members
  or on **schedulable nodes outside every pool** (such as an always-on base). The machine
  stays on if they don't fit.
- **Protecting running work:** give that work (e.g. the Jobs) the annotation
  `hardware-autoscaler.safewords.com/safe-to-evict: "false"`. A node running such a pod is
  never considered idle.
- **Guests you can't interrupt:** guest pods are evicted at power-off, so a CI runner in the
  middle of a job fails. Either annotate such pods `safe-to-evict: "false"`, or keep them off
  the machine entirely with a `NoSchedule` taint that only the pool's workloads tolerate.
  DaemonSets that must run there (CNI and CSI agents, etc.) need the toleration too.

### Node fencing

Just before powering a machine off (or putting it into standby), the operator annotates its Node with
`hardware-autoscaler.safewords.com/powered-off: <RFC 3339 time>`. Tools that fence
unresponsive nodes, for example by adding the `node.kubernetes.io/out-of-service` taint,
should **skip nodes carrying this annotation**: an intentionally powered-off node isn't a
failed one.

When the operator powers the machine back on and its Node is Ready, it removes the
annotation. It also removes any `out-of-service` taint, but only from nodes carrying its own
annotation, so it never undoes a fence placed on a genuinely failed node.

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
| Machines never scale down | `kubectl get nodescalingpool -o yaml`: `status.recentDecisions` and `status.message` explain holds (pods pending, holding after power-on, ...). Nodes with bare or `safe-to-evict=false` pods are never candidates. |
| The wrong machine was powered on or off | `status.recentDecisions` shows the ranked candidates and the rule that decided (weight, S3 capability, size, name). See "Preferred machines". |
| Condition `PreferredNodesValid=False` | A `scaleUp.preferredNodes` entry names no Node, or a Node the pool's `nodeSelector` doesn't select. Fix or remove it; the other entries still apply. |
| `kubectl apply` of a pool fails with `failed calling webhook` | The operator isn't serving (crashed or starting). Wait for it, or delete the `ValidatingWebhookConfiguration` to change pools without validation. |
| Nothing happens at all | Check `operator.dryRun`, `powerPolicy: Auto`, and that the Node carries the pool's labels (`kubectl get npmc` shows the POOL; check the `PoolMembership` condition). |
| `status.interfaceWarnings` is not empty | A higher-priority interface failed and a fallback took over. Fix the listed interface. |
| JetKVM: `no retained state on .../atx/state` | MQTT disabled on the JetKVM, wrong `baseTopic` (it must include the device ID), or the ATX/DC extension is not active. |
| JetKVM: commands have no effect | "Enable actions" is off in the JetKVM MQTT settings. |
| Condition `NodeFound=False` | `spec.nodeName` doesn't match any Node. Fix the name (it must also equal `metadata.name`). |
| A machine stays on although one pool finds it idle | Another pool selecting it still needs it: the decision says `kept: needed by pool <name>: <reason>`, and that pool's `status.needed` has the reason. See "Overlapping pools". |
| Condition `IdentityVerified=False` | An interface reports a different system UUID than the Node: it points at another machine. That interface is disabled. Fix its address. |
| Condition `PowerStateConsistent=False` | An interface reports Off although the Node is alive, so it's probably wired to another machine. No power actions are taken until it agrees. |
