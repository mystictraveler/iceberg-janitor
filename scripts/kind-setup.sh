#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${1:-iceberg-janitor}"

echo "==> Creating kind cluster: ${CLUSTER_NAME}"

# Check prerequisites
for cmd in kind kubectl docker; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: $cmd is required but not installed."
        exit 1
    fi
done

# Delete existing cluster if it exists
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "==> Cluster ${CLUSTER_NAME} already exists. Deleting..."
    kind delete cluster --name "${CLUSTER_NAME}"
fi

# Create cluster with port mappings for MinIO and REST catalog
cat <<EOF | kind create cluster --name "${CLUSTER_NAME}" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30900
        hostPort: 9000
        protocol: TCP
      - containerPort: 30901
        hostPort: 9001
        protocol: TCP
      - containerPort: 30818
        hostPort: 8181
        protocol: TCP
      - containerPort: 30092
        hostPort: 19092
        protocol: TCP
EOF

echo "==> Cluster created. Context set to kind-${CLUSTER_NAME}"

# Wait for node to be ready
echo "==> Waiting for node to be ready..."
kubectl wait --for=condition=Ready nodes --all --timeout=120s

echo "==> Kind cluster '${CLUSTER_NAME}' is ready!"
echo ""
echo "Next steps:"
echo "  1. Build the janitor image:  make docker-build"
echo "  2. Load into kind:           kind load docker-image iceberg-janitor:latest --name ${CLUSTER_NAME}"
echo "  3. Deploy dev stack:         ./scripts/dev-stack-up.sh"
