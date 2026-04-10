#!/bin/bash
set -euo pipefail

# --- Install Go ---
curl -fsSL "https://go.dev/dl/go${go_version}.linux-amd64.tar.gz" | tar -C /usr/local -xz
export PATH=$PATH:/usr/local/go/bin:/root/go/bin
export HOME=/root
export GOPATH=/root/go
echo 'export PATH=$PATH:/usr/local/go/bin:/root/go/bin' >> /etc/profile.d/go.sh

# --- Install git + clone repo ---
dnf install -y git
cd /opt
git clone --branch "${repo_branch}" --depth 1 "${repo_url}" iceberg-janitor
cd iceberg-janitor/go

# --- Build all binaries ---
CGO_ENABLED=0 go build -o /usr/local/bin/janitor-cli      ./cmd/janitor-cli
CGO_ENABLED=0 go build -o /usr/local/bin/janitor-streamer  ./cmd/janitor-streamer
CGO_ENABLED=0 go build -o /usr/local/bin/janitor-server    ./cmd/janitor-server

# --- Write env file for bench convenience ---
cat > /etc/profile.d/janitor-bench.sh <<ENVEOF
export AWS_REGION="${region}"
export JANITOR_WAREHOUSE_URL="${warehouse_url}"
export JANITOR_API_URL="${api_gateway_url}"
export BENCH_BUCKET="${bucket_name}"
ENVEOF

# --- Copy bench scripts ---
cp /opt/iceberg-janitor/go/test/bench/*.sh /usr/local/bin/ 2>/dev/null || true
chmod +x /usr/local/bin/*.sh 2>/dev/null || true

echo "=== iceberg-janitor bench runner ready ==="
