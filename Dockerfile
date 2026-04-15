FROM rust:1.94 AS builder
WORKDIR /app

# Cache dependencies: copy manifests first, build a dummy to cache deps
COPY Cargo.toml Cargo.lock ./
COPY crates/exspeed/Cargo.toml crates/exspeed/Cargo.toml
COPY crates/exspeed-common/Cargo.toml crates/exspeed-common/Cargo.toml
COPY crates/exspeed-protocol/Cargo.toml crates/exspeed-protocol/Cargo.toml
COPY crates/exspeed-streams/Cargo.toml crates/exspeed-streams/Cargo.toml
COPY crates/exspeed-storage/Cargo.toml crates/exspeed-storage/Cargo.toml
COPY crates/exspeed-broker/Cargo.toml crates/exspeed-broker/Cargo.toml
COPY crates/exspeed-api/Cargo.toml crates/exspeed-api/Cargo.toml
COPY crates/exspeed-connectors/Cargo.toml crates/exspeed-connectors/Cargo.toml
COPY crates/exspeed-processing/Cargo.toml crates/exspeed-processing/Cargo.toml

# Create dummy source files so cargo can resolve the workspace
RUN mkdir -p crates/exspeed/src && echo "fn main() {}" > crates/exspeed/src/main.rs && touch crates/exspeed/src/lib.rs \
    && mkdir -p crates/exspeed-common/src && touch crates/exspeed-common/src/lib.rs \
    && mkdir -p crates/exspeed-protocol/src && touch crates/exspeed-protocol/src/lib.rs \
    && mkdir -p crates/exspeed-streams/src && touch crates/exspeed-streams/src/lib.rs \
    && mkdir -p crates/exspeed-storage/src && touch crates/exspeed-storage/src/lib.rs \
    && mkdir -p crates/exspeed-broker/src && touch crates/exspeed-broker/src/lib.rs \
    && mkdir -p crates/exspeed-api/src && touch crates/exspeed-api/src/lib.rs \
    && mkdir -p crates/exspeed-connectors/src && touch crates/exspeed-connectors/src/lib.rs \
    && mkdir -p crates/exspeed-processing/src && touch crates/exspeed-processing/src/lib.rs

# Build dependencies only (cached unless Cargo.toml/Cargo.lock change)
RUN cargo build --release 2>/dev/null || true

# Now copy the real source and build
COPY . .
# Touch all source files to ensure cargo rebuilds them (not the cached dummy)
RUN find crates -name "*.rs" -exec touch {} +
RUN cargo build --release

# Runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/exspeed /usr/local/bin/exspeed
EXPOSE 5933 8080
VOLUME /var/lib/exspeed
ENTRYPOINT ["exspeed"]
CMD ["server", "--data-dir", "/var/lib/exspeed", "--bind", "0.0.0.0:5933", "--api-bind", "0.0.0.0:8080"]
