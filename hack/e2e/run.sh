#!/usr/bin/env sh
# End-to-end test on a local 2-node minikube cluster (profile "kha-e2e").
#
# The worker node (kha-e2e-m02) is "powered" by a fake JetKVM speaking the
# real JetKVM MQTT protocol; its first power interface is an unreachable IPMI
# BMC, so every operation exercises the fallback chain. The test checks:
#   0. the API server rejects a NodePowerManagementConfig whose name != nodeName and a
#      NodeScalingPool with an empty nodeSelector (CEL rules)
#   1. pool membership comes from the Node label; the idle worker is scaled
#      down: cordon -> drain -> PowerOff via jetkvm
#   2. a pending pod that only fits the worker scales it up again:
#      PowerOn via jetkvm -> uncordon -> pod runs
#   3. safety gates: an interface that reports Off for a live Node, and a
#      NodePowerManagementConfig without a Node, are observed but never acted on
#   4. a Node selected by two pools is a conflict and belongs to neither
#   5. demand removed -> scaled down again; deleting the pool clears its decision
#
# Prerequisites: docker, minikube, kubectl, helm, and the image built with
#   docker build -t kube-hardware-autoscaler:dev .
# Start the cluster with:
#   minikube start -p kha-e2e --nodes 2 --driver=docker
set -eu
cd "$(dirname "$0")/../.."

P=kha-e2e
CP=kha-e2e          # control-plane node: runs the operator and fixtures
W=kha-e2e-m02       # managed worker
k() { kubectl --context "$P" "$@"; }
step() { printf '\n=== %s\n' "$*"; }
jp() { k get nodepowermanagementconfig "$W" -o jsonpath="$1"; }
cond() { # cond <nodepowermanagementconfig> <type> -> "status/reason"
  k get nodepowermanagementconfig "$1" -o jsonpath="{.status.conditions[?(@.type==\"$2\")].status}/{.status.conditions[?(@.type==\"$2\")].reason}"
}
fail() { echo "FAIL: $*"; k get nodepowermanagementconfigs,nodescalingpools -o wide || true; exit 1; }
presses() { k -n kube-hardware-autoscaler logs deploy/fake-jetkvm | grep -c '^command' || true; }

wait_for() { # wait_for <description> <timeout-seconds> <command...>
  desc=$1; timeout=$2; shift 2
  i=0
  until "$@" >/dev/null 2>&1; do
    i=$((i + 2))
    if [ "$i" -ge "$timeout" ]; then
      echo "TIMEOUT waiting for: $desc"
      k get nodepowermanagementconfigs,nodescalingpools -o wide || true
      k -n kube-hardware-autoscaler logs deploy/kube-hardware-autoscaler --tail=60 || true
      exit 1
    fi
    sleep 2
  done
  echo "ok: $desc"
}
phase_is() { [ "$(jp '{.status.phase}')" = "$1" ]; }
cond_is() { [ "$(cond "$1" "$2")" = "$3" ]; }

step "load image and install chart"
# Tag by image id: minikube does not replace an in-use tag when reloading.
TAG="dev-$(docker image inspect -f '{{.Id}}' kube-hardware-autoscaler:dev | cut -c8-19)"
docker tag kube-hardware-autoscaler:dev "kube-hardware-autoscaler:$TAG"
minikube -p "$P" image load "kube-hardware-autoscaler:$TAG"
k create namespace kube-hardware-autoscaler --dry-run=client -o yaml | k apply -f -
# Helm only installs crds/ on first install; keep them current like users must.
k apply --server-side --force-conflicts -f charts/kube-hardware-autoscaler/crds/
helm --kube-context "$P" upgrade --install kube-hardware-autoscaler charts/kube-hardware-autoscaler -n kube-hardware-autoscaler \
  --set image.repository=kube-hardware-autoscaler --set "image.tag=$TAG" --set image.pullPolicy=Never \
  --set "nodeSelector.kubernetes\.io/hostname=$CP" \
  --set operator.logLevel=info\\,kube_hardware_autoscaler=debug
k -n kube-hardware-autoscaler apply -f hack/e2e/fake-jetkvm.yaml
k -n kube-hardware-autoscaler rollout status deploy/kube-hardware-autoscaler --timeout=180s
k -n kube-hardware-autoscaler rollout status deploy/mosquitto --timeout=180s
k -n kube-hardware-autoscaler rollout status deploy/fake-jetkvm --timeout=180s

step "validate documented examples against the CRDs"
k apply --dry-run=server -f examples/ >/dev/null && echo "ok: examples/ accepted"

step "0. CEL rules reject unsafe objects"
bad_mn=$(printf '%s\n' 'apiVersion: hardware-autoscaler.safewords.com/v1alpha1' 'kind: NodePowerManagementConfig' 'metadata: {name: not-the-node}' \
  'spec: {nodeName: kha-e2e-m02, powerInterfaces: [{driver: wakeOnLan, config: {macAddress: "aa:bb:cc:dd:ee:ff"}}]}' |
  k apply --dry-run=server -f - 2>&1 || true)
echo "$bad_mn" | grep -q "metadata.name must equal spec.nodeName" || fail "name != nodeName was not rejected: $bad_mn"
echo "ok: NodePowerManagementConfig with name != nodeName rejected"
bad_pool=$(printf '%s\n' 'apiVersion: hardware-autoscaler.safewords.com/v1alpha1' 'kind: NodeScalingPool' 'metadata: {name: everything}' \
  'spec: {nodeSelector: {}}' | k apply --dry-run=server -f - 2>&1 || true)
echo "$bad_pool" | grep -q "spec.nodeSelector must not be empty" || fail "empty nodeSelector was not rejected: $bad_pool"
echo "ok: NodeScalingPool with empty nodeSelector rejected"

step "1. membership by label; idle worker scaled down through the fallback chain"
k label node "$W" hardware-autoscaler.safewords.com/pool=e2e --overwrite
k apply -f hack/e2e/scenario.yaml
wait_for "worker is a member of pool e2e" 60 cond_is "$W" PoolMembership True/InPool
[ "$(jp '{.status.pool}')" = e2e ] || fail "expected status.pool=e2e"
wait_for "worker powered off" 240 phase_is Off
[ "$(jp '{.status.lastPowerAction.via}')" = kvm ] || fail "expected via=kvm"
jp '{.status.interfaceWarnings}' | grep -q '^\["bmc: ' || fail "expected a bmc interface warning"
[ "$(k get node "$W" -o jsonpath='{.spec.unschedulable}')" = true ] || fail "expected worker cordoned"
[ "$(jp '{.status.scalingDecision.pool}')" = e2e ] || fail "decision must carry the pool name"
echo "ok: via=kvm, bmc warning recorded, node cordoned, decision stamped with pool"

step "2. pending pod scales the worker back up"
k create deployment e2e-demand --image=registry.k8s.io/pause:3.9 --dry-run=client -o yaml | k apply -f -
k patch deployment e2e-demand --type merge -p "{\"spec\":{\"template\":{\"spec\":{\"nodeSelector\":{\"kubernetes.io/hostname\":\"$W\"},\"containers\":[{\"name\":\"pause\",\"image\":\"registry.k8s.io/pause:3.9\",\"resources\":{\"requests\":{\"cpu\":\"100m\"}}}]}}}}"
wait_for "worker powered on" 180 phase_is On
wait_for "worker uncordoned" 60 sh -c "[ -z \"\$(kubectl --context $P get node $W -o jsonpath='{.spec.unschedulable}')\" ]"
wait_for "demand pod running on worker" 120 sh -c "kubectl --context $P get pods -l app=e2e-demand -o jsonpath='{.items[0].status.phase}' | grep -q Running"

step "3. safety gates: wrong machine and missing Node are never acted on"
before=$(presses)
# The "wrong machine" interface reports Off although its Node (the control plane) is alive.
k -n kube-hardware-autoscaler exec deploy/mosquitto -- mosquitto_pub -h localhost -r -t jetkvm/wrong-machine/atx/state -m '{"power":false,"hdd":false}'
k apply -f hack/e2e/safety.yaml
wait_for "wrong-machine interface flagged" 90 cond_is "$CP" PowerStateConsistent False/InterfaceReportsOffButNodeIsAlive
wait_for "missing Node flagged" 90 cond_is ghost NodeFound False/NodeNotFound
sleep 20   # give the operator several reconciles to (wrongly) act
[ -z "$(k get nodepowermanagementconfig "$CP" -o jsonpath='{.status.lastPowerAction}')" ] || fail "power action issued despite inconsistent state"
[ -z "$(k get nodepowermanagementconfig ghost -o jsonpath='{.status.lastPowerAction}')" ] || fail "power action issued for a missing Node"
[ "$(presses)" = "$before" ] || fail "a KVM button was pressed by a gated NodePowerManagementConfig"
[ "$(cond "$W" IdentityVerified)" = "Unknown/NotVerifiable" ] || fail "jetkvm cannot prove identity; expected Unknown/NotVerifiable"
echo "ok: AlwaysOn with an Off-reporting interface and AlwaysOff without a Node took no action"
k delete -f hack/e2e/safety.yaml

step "4. a Node selected by two pools belongs to neither"
printf '%s\n' 'apiVersion: hardware-autoscaler.safewords.com/v1alpha1' 'kind: NodeScalingPool' 'metadata: {name: e2e-dup}' \
  'spec: {nodeSelector: {matchLabels: {hardware-autoscaler.safewords.com/pool: e2e}}}' | k apply -f -
wait_for "worker reported as conflict" 60 cond_is "$W" PoolMembership False/Conflict
wait_for "pool e2e lists the conflict" 60 sh -c "kubectl --context $P get nodescalingpool e2e -o jsonpath='{.status.conflicts}' | grep -q $W"
k delete nodescalingpool e2e-dup
wait_for "worker back in pool e2e" 60 cond_is "$W" PoolMembership True/InPool

step "5. demand removed -> scaled down; deleting the pool clears its decision"
k delete deployment e2e-demand
wait_for "worker powered off again" 240 phase_is Off
k delete nodescalingpool e2e
wait_for "worker no longer in a pool" 90 cond_is "$W" PoolMembership False/NotInPool
wait_for "stale decision cleared" 60 sh -c "[ -z \"\$(kubectl --context $P get nodepowermanagementconfig $W -o jsonpath='{.status.scalingDecision}')\" ]"

step "events"
k get events -A --field-selector involvedObject.kind=NodePowerManagementConfig --sort-by=.lastTimestamp | tail -15
step "fake jetkvm log"
k -n kube-hardware-autoscaler logs deploy/fake-jetkvm | tail -10
printf '\nE2E PASSED\n'
