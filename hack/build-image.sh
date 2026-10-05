#!/usr/bin/env sh
# Builds the image locally the way CI does: a static musl binary first (here
# inside rust:alpine, so no musl toolchain is needed on the host), placed at
# dist/<arch>/, then the runtime-only Dockerfile.
#
#   sh hack/build-image.sh                 # tag kube-hardware-autoscaler:dev
#   IMAGE=my/image:tag sh hack/build-image.sh
# linux/amd64 only, as published.
set -eu
cd "$(dirname "$0")/.."
arch=amd64
image=${IMAGE:-kube-hardware-autoscaler:dev}
docker run --rm --platform "linux/$arch" -v "$PWD":/src -w /src \
  -e CARGO_TARGET_DIR=/src/target/musl-$arch \
  rust:1-alpine \
  sh -c 'apk add --no-cache musl-dev gcc g++ make cmake perl linux-headers >/dev/null && cargo build --release --locked'
mkdir -p "dist/$arch"
cp "target/musl-$arch/release/kube-hardware-autoscaler" "dist/$arch/kube-hardware-autoscaler"
docker buildx build --platform "linux/$arch" --load -t "$image" .
echo "built $image"
