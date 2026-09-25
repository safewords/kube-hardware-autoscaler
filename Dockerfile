# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
# Cache dependencies separately from the application sources.
RUN mkdir src && echo 'fn main() {}' > src/main.rs && touch src/lib.rs \
    && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/kube-hardware-autoscaler /usr/local/bin/kube-hardware-autoscaler
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/kube-hardware-autoscaler"]
CMD ["run"]
