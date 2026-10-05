# syntax=docker/dockerfile:1
#
# Runtime image only, linux/amd64: nothing is compiled here. CI builds the
# static musl binary (x86_64-unknown-linux-musl) on the runner and places it in
# the build context at dist/amd64/kube-hardware-autoscaler (dist/${TARGETARCH}).
# For a local image, run `sh hack/build-image.sh`, which compiles the binary in
# rust:1-alpine first.
#
# The same image runs the operator, the Wake-on-LAN relay pods
# (`kube-hardware-autoscaler wake`), and, by default, the in-band shutdown,
# suspend and sleep-probe pods (`sh -c ...` with nsenter into the host's PID 1
# namespaces, as root).
FROM alpine:3.24
# ca-certificates: the system trust store rustls uses (API server, Redfish,
#   PiKVM/NanoKVM HTTPS, MQTT over TLS).
# util-linux-misc: nsenter, for the shutdown/suspend pods (the only reason
#   for it; busybox provides sh, cat and date).
# No tzdata: the operator only uses UTC.
RUN apk add --no-cache ca-certificates util-linux-misc
ARG TARGETARCH
COPY dist/${TARGETARCH}/kube-hardware-autoscaler /usr/local/bin/kube-hardware-autoscaler
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/kube-hardware-autoscaler"]
CMD ["run"]
