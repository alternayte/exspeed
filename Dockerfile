# syntax=docker/dockerfile:1

# --- Dependency planning (cargo-chef) -------------------------------------
# Caches the dependency build in its own layer, keyed on the lockfile and
# manifests only, so source edits don't recompile ~400 crates.
FROM rust:1.97 AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release -p exspeed --recipe-path recipe.json
COPY . .
RUN cargo build --release -p exspeed

# --- Runtime image ----------------------------------------------------------
FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 1000 exspeed \
 && useradd --system --uid 1000 --gid 1000 --home-dir /var/lib/exspeed --shell /usr/sbin/nologin exspeed \
 && mkdir -p /var/lib/exspeed \
 && chown -R exspeed:exspeed /var/lib/exspeed
COPY --from=builder /app/target/release/exspeed /usr/local/bin/exspeed
USER 1000:1000
# 5933 = binary protocol, 8080 = HTTP API, 5934 = replication (multi-pod)
EXPOSE 5933 8080 5934
VOLUME /var/lib/exspeed
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD ["exspeed", "healthcheck"]
ENTRYPOINT ["exspeed"]
# Listeners are left to the defaults (0.0.0.0:5933 / 0.0.0.0:8080) so that
# EXSPEED_BIND / EXSPEED_API_BIND or a config file can change them; flags
# would override both, and `exspeed healthcheck` derives its URL from the
# same environment and config file.
CMD ["server", "--data-dir", "/var/lib/exspeed"]
