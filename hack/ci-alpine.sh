#!/usr/bin/env bash
# Runs a shell command in the pinned Rust Alpine image, natively musl, with the
# workspace mounted at the same path (so compiler caches see stable paths).
#
#   hack/ci-alpine.sh 'cargo test --locked'
#
# CI uses it for every cargo command: tests, clippy and the release binary all
# build on Alpine, so the published binary is the one the tests ran against.
#
# Compile cache: when the runner provides `ci-cache-setup` (it sets sccache up
# against a shared cache and writes RUSTC_WRAPPER and SCCACHE_* to a file) and
# a static `sccache`, the setup runs here on the runner, where the job's OIDC
# token can be fetched, and its settings and the sccache binary are handed to
# the container. Without them the build is uncached.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=${RUST_ALPINE_IMAGE:-rust:1.99.0-alpine3.24}
ws=$(pwd)
tools="${RUNNER_TEMP:-/tmp}/ci-alpine-tools"
mkdir -p "$tools"

args=(--rm -v "$ws:$ws" -w "$ws" -v "$tools:/ci-tools:ro" -e CARGO_TERM_COLOR=always)
if command -v ci-cache-setup >/dev/null && command -v sccache >/dev/null; then
  cp "$(command -v sccache)" "$tools/"
  command -v ci-cache-stats >/dev/null && cp "$(command -v ci-cache-stats)" "$tools/"
  : > "$tools/env"
  GITHUB_ENV="$tools/env" ci-cache-setup
  sccache --stop-server >/dev/null 2>&1 || true
  # sccache runs from /ci-tools in the container.
  [ -s "$tools/env" ] && args+=(--env-file "$tools/env")
fi

inner=$(cat <<'EOF'
set -eu
# aws-lc-sys and ring compile C; nothing links OpenSSL.
apk add --no-cache -q musl-dev gcc g++ make cmake perl linux-headers git bash jq >/dev/null
export PATH=/ci-tools:$PATH
status=0
sh -c "$CI_ALPINE_CMD" || status=$?
if [ -x /ci-tools/ci-cache-stats ]; then GITHUB_STEP_SUMMARY= ci-cache-stats || true; fi
# Hand the workspace back to the runner's user.
chown -R "$CI_ALPINE_OWNER" "$PWD" 2>/dev/null || true
exit $status
EOF
)
docker run "${args[@]}" -e CI_ALPINE_CMD="$*" -e CI_ALPINE_OWNER="$(id -u):$(id -g)" "$IMAGE" sh -c "$inner"
