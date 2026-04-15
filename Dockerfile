FROM rust:1.94 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/exspeed /usr/local/bin/exspeed
EXPOSE 5933 8080
VOLUME /var/lib/exspeed
ENTRYPOINT ["exspeed"]
CMD ["server", "--data-dir", "/var/lib/exspeed", "--bind", "0.0.0.0:5933", "--api-bind", "0.0.0.0:8080"]
