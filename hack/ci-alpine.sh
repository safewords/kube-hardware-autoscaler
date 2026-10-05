#!/usr/bin/env bash
# Runs a shell command in the pinned Rust Alpine image, natively musl, with the
# workspace mounted at the same path (so compiler caches see stable paths).
#
#   hack/ci-alpine.sh 'cargo test --locked'
#
# CI uses it for every cargo command: tests, clippy and the release binary all
# build on Alpine, so the published binary is the one the tests ran against.
#
# The container is a local "builder" image: the pinned Rust Alpine image plus
# the C toolchain the build needs (aws-lc-sys and ring compile C; nothing
# links OpenSSL).
#
# Compile cache: when the runner provides `ci-cache-setup` (it sets sccache up
# against a shared S3 cache and writes RUSTC_WRAPPER, SCCACHE_* and the S3
# credentials to a file) and a static `sccache`, the setup runs here on the
# runner, where the job's OIDC token can be fetched, and its settings and the
# sccache binary are handed to the container. The builder image is kept in the
# same S3 bucket (a `docker save`, under ci-images/), so a job loads it over
# the local network instead of pulling and apk-installing it every time; only
# a job allowed to write the cache stores it. Without any of this the build
# runs uncached and the builder image is built locally.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE=${RUST_ALPINE_IMAGE:-rust:1.99.0-alpine3.24}
APK="musl-dev gcc g++ make cmake perl linux-headers git bash jq"
key=$(printf '%s\n%s\n' "$BASE" "$APK" | sha256sum | cut -c1-16)
BUILDER="ci-alpine-builder:$key"
ws=$(pwd)
tools="${RUNNER_TEMP:-/tmp}/ci-alpine-tools"
mkdir -p "$tools"

args=(--rm -v "$ws:$ws" -w "$ws" -v "$tools:/ci-tools:ro" -e CARGO_TERM_COLOR=always)
cache=0
if command -v ci-cache-setup >/dev/null && command -v sccache >/dev/null; then
  cp "$(command -v sccache)" "$tools/"
  if command -v ci-cache-stats >/dev/null; then cp "$(command -v ci-cache-stats)" "$tools/"; fi
  : > "$tools/env"
  GITHUB_ENV="$tools/env" ci-cache-setup
  sccache --stop-server >/dev/null 2>&1 || true
  if [ -s "$tools/env" ]; then args+=(--env-file "$tools/env"); cache=1; fi
fi

# The builder image: already here, or from the cache, or built (and stored).
if ! docker image inspect "$BUILDER" >/dev/null 2>&1; then
  loaded=0
  if [ "$cache" = 1 ] && command -v aws >/dev/null; then
    set -a; . "$tools/env"; set +a
    export AWS_CONFIG_FILE="$tools/aws-config" AWS_DEFAULT_REGION="$SCCACHE_REGION"
    printf '[default]\ns3 =\n    addressing_style = path\n' > "$AWS_CONFIG_FILE"
    obj="s3://$SCCACHE_BUCKET/ci-images/$key.tar.zst"
    s3() { aws --endpoint-url "$SCCACHE_ENDPOINT" s3 "$@"; }
    if s3 cp --only-show-errors "$obj" - 2>/dev/null | zstd -dc | docker load -q; then
      loaded=1
      echo "builder image $BUILDER loaded from the cache"
    fi
  fi
  if [ "$loaded" = 0 ]; then
    printf 'FROM %s\nRUN apk add --no-cache %s\n' "$BASE" "$APK" | docker build -q -t "$BUILDER" - >/dev/null
    echo "builder image $BUILDER built"
    if [ "$cache" = 1 ] && command -v aws >/dev/null && [ "${SCCACHE_S3_RW_MODE:-}" = READ_WRITE ]; then
      docker save "$BUILDER" | zstd -q -T0 -3 | s3 cp --only-show-errors - "$obj" \
        && echo "builder image stored in the cache" || echo "could not store the builder image (not fatal)"
    fi
  fi
fi

inner=$(cat <<'EOF'
set -eu
export PATH=/ci-tools:$PATH
status=0
sh -c "$CI_ALPINE_CMD" || status=$?
if [ -x /ci-tools/ci-cache-stats ]; then GITHUB_STEP_SUMMARY= ci-cache-stats || true; fi
# Hand the workspace back to the runner's user.
chown -R "$CI_ALPINE_OWNER" "$PWD" 2>/dev/null || true
exit $status
EOF
)
docker run "${args[@]}" -e CI_ALPINE_CMD="$*" -e CI_ALPINE_OWNER="$(id -u):$(id -g)" "$BUILDER" sh -c "$inner"
