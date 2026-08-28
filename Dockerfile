FROM rust:1.98-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p roze-dtm-service --locked

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /app roze
WORKDIR /app
COPY --from=builder /build/target/release/roze-dtm-service /usr/local/bin/roze-dtm-service
COPY service/config.production.yaml /app/config.yaml
ENV ROZE_CONFIG_PATH=/app/config.yaml
EXPOSE 8090 36790
USER 10001
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:8090/healthz > /dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/roze-dtm-service"]
