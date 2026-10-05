#!/usr/bin/env bash
# Runs a shell command in the pinned Rust Alpine image, natively musl, with the
# workspace mounted at the same path (so compiler caches see stable paths).
#
#   hack/ci-alpine.sh 'cargo test --locked'
#
# CI uses it for every cargo command: tests, clippy and the release binary all
# build on Alpine, so the published binary is the one the tests ran against.
#
# Compile cache: when the runner provides `sccache` and `ci-cache-setup` (an
# sccache setup script that writes RUSTC_WRAPPER and SCCACHE_* to a file),
# both are copied into the container and the cache's settings and the job's
# OIDC token variables are passed through. Without them the build is uncached.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=${RUST_ALPINE_IMAGE:-rust:1.99.0-alpine3.24}
ws=$(pwd)
tools="${RUNNER_TEMP:-/tmp}/ci-alpine-tools"
mkdir -p "$tools"

args=(--rm -v "$ws:$ws" -w "$ws" -v "$tools:/ci-tools:ro" -e CARGO_TERM_COLOR=always)
for t in sccache ci-cache-setup ci-cache-stats; do
  if p=$(command -v "$t"); then cp "$p" "$tools/"; fi
done
# The compile cache's settings and the OIDC token request (for write access).
for v in $(compgen -e | grep -E '^(SAFEWORDS_CI_CACHE_|ACTIONS_ID_TOKEN_REQUEST_)' || true); do
  args+=(-e "$v")
done

inner=$(cat <<'EOF'
set -eu
# aws-lc-sys and ring compile C; nothing links OpenSSL.
apk add --no-cache -q musl-dev gcc g++ make cmake perl linux-headers git bash curl jq >/dev/null
export PATH=/ci-tools:$PATH
if [ -x /ci-tools/ci-cache-setup ] && [ -x /ci-tools/sccache ]; then
  export GITHUB_ENV=/tmp/ci-cache.env
  : > "$GITHUB_ENV"
  ci-cache-setup
  set -a; . "$GITHUB_ENV"; set +a
fi
status=0
sh -c "$CI_ALPINE_CMD" || status=$?
if [ -x /ci-tools/ci-cache-stats ]; then GITHUB_STEP_SUMMARY= ci-cache-stats || true; fi
# Hand the workspace back to the runner's user.
chown -R "$CI_ALPINE_OWNER" "$PWD" 2>/dev/null || true
exit $status
EOF
)
docker run "${args[@]}" -e CI_ALPINE_CMD="$*" -e CI_ALPINE_OWNER="$(id -u):$(id -g)" "$IMAGE" sh -c "$inner"
