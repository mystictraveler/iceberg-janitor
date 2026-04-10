# Multi-stage build for iceberg-janitor Go binaries.
# Produces a minimal image with janitor-server, janitor-cli, and
# janitor-streamer (the bench writer). No CGO dependencies.
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /out/janitor-server  ./cmd/janitor-server \
 && CGO_ENABLED=0 go build -o /out/janitor-cli     ./cmd/janitor-cli \
 && CGO_ENABLED=0 go build -o /out/janitor-streamer ./cmd/janitor-streamer

# --- Server image (default) ---
FROM gcr.io/distroless/static-debian12 AS server
COPY --from=build /out/janitor-server  /janitor-server
COPY --from=build /out/janitor-cli     /janitor-cli
COPY --from=build /out/janitor-streamer /janitor-streamer
EXPOSE 8080
ENTRYPOINT ["/janitor-server"]

# --- Bench image (has bash, awscli, bench scripts) ---
FROM debian:bookworm-slim AS bench
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl unzip gawk && \
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscli.zip && \
    unzip -q /tmp/awscli.zip -d /tmp && \
    /tmp/aws/install && \
    rm -rf /tmp/awscli.zip /tmp/aws && \
    apt-get remove -y unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/janitor-server  /usr/local/bin/janitor-server
COPY --from=build /out/janitor-cli     /usr/local/bin/janitor-cli
COPY --from=build /out/janitor-streamer /usr/local/bin/janitor-streamer
COPY test/bench/ /bench/
RUN chmod +x /bench/*.sh
WORKDIR /bench
ENTRYPOINT ["/bench/bench-tpcds-athena.sh"]
