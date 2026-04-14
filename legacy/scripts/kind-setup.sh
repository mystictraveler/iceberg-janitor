#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${1:-iceberg-janitor}"

# Knative versions
KNATIVE_SERVING_VERSION="v1.17.0"
KNATIVE_EVENTING_VERSION="v1.17.0"
KNATIVE_KAFKA_VERSION="v1.17.0"

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

# ---------------------------------------------------------------------------
# Install Knative Serving
# ---------------------------------------------------------------------------
echo "==> Installing Knative Serving CRDs (${KNATIVE_SERVING_VERSION})..."
kubectl apply -f "https://github.com/knative/serving/releases/download/knative-${KNATIVE_SERVING_VERSION}/serving-crds.yaml"

echo "==> Installing Knative Serving core (${KNATIVE_SERVING_VERSION})..."
kubectl apply -f "https://github.com/knative/serving/releases/download/knative-${KNATIVE_SERVING_VERSION}/serving-core.yaml"

# ---------------------------------------------------------------------------
# Install Kourier networking layer
# ---------------------------------------------------------------------------
echo "==> Installing Kourier networking layer..."
kubectl apply -f "https://github.com/knative/net-kourier/releases/download/knative-${KNATIVE_SERVING_VERSION}/kourier.yaml"

echo "==> Configuring Knative to use Kourier..."
kubectl patch configmap/config-network \
    --namespace knative-serving \
    --type merge \
    --patch '{"data":{"ingress-class":"kourier.ingress.networking.knative.dev"}}'

# ---------------------------------------------------------------------------
# Configure default domain (sslip.io for local development)
# ---------------------------------------------------------------------------
echo "==> Configuring default domain..."
kubectl apply -f "https://github.com/knative/serving/releases/download/knative-${KNATIVE_SERVING_VERSION}/serving-default-domain.yaml"

# ---------------------------------------------------------------------------
# Install Knative Eventing
# ---------------------------------------------------------------------------
echo "==> Installing Knative Eventing CRDs (${KNATIVE_EVENTING_VERSION})..."
kubectl apply -f "https://github.com/knative/eventing/releases/download/knative-${KNATIVE_EVENTING_VERSION}/eventing-crds.yaml"

echo "==> Installing Knative Eventing core (${KNATIVE_EVENTING_VERSION})..."
kubectl apply -f "https://github.com/knative/eventing/releases/download/knative-${KNATIVE_EVENTING_VERSION}/eventing-core.yaml"

# ---------------------------------------------------------------------------
# Install Knative Kafka Source controller
# ---------------------------------------------------------------------------
echo "==> Installing Knative Kafka source controller (${KNATIVE_KAFKA_VERSION})..."
kubectl apply -f "https://github.com/knative-extensions/eventing-kafka-broker/releases/download/knative-${KNATIVE_KAFKA_VERSION}/eventing-kafka-controller.yaml"
kubectl apply -f "https://github.com/knative-extensions/eventing-kafka-broker/releases/download/knative-${KNATIVE_KAFKA_VERSION}/eventing-kafka-source.yaml"

# ---------------------------------------------------------------------------
# Wait for all Knative pods to be ready
# ---------------------------------------------------------------------------
echo "==> Waiting for Knative Serving pods to be ready..."
kubectl wait --for=condition=ready pod --all -n knative-serving --timeout=300s

echo "==> Waiting for Knative Eventing pods to be ready..."
kubectl wait --for=condition=ready pod --all -n knative-eventing --timeout=300s

echo "==> Kind cluster '${CLUSTER_NAME}' is ready with Knative!"
echo ""
echo "Next steps:"
echo "  1. Build the janitor image:  make docker-build"
echo "  2. Load into kind:           kind load docker-image iceberg-janitor:latest --name ${CLUSTER_NAME}"
echo "  3. Deploy dev stack:         ./scripts/dev-stack-up.sh"
