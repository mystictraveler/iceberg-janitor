#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "==> Deploying dev stack to kind cluster..."

# Apply kustomize manifests
kubectl apply -k "${PROJECT_DIR}/manifests/dev"

# Wait for MinIO
echo "==> Waiting for MinIO..."
kubectl -n iceberg-janitor wait --for=condition=ready pod -l app=minio --timeout=120s

# Create the warehouse bucket in MinIO
echo "==> Creating warehouse bucket in MinIO..."
kubectl -n iceberg-janitor run minio-setup --rm -i --restart=Never \
    --image=minio/mc:latest -- \
    sh -c '
        mc alias set local http://minio:9000 admin password &&
        mc mb local/warehouse --ignore-existing &&
        echo "Bucket created successfully"
    '

# Wait for Kafka
echo "==> Waiting for Kafka..."
kubectl -n iceberg-janitor wait --for=condition=ready pod -l app=kafka --timeout=180s

# Wait for REST catalog
echo "==> Waiting for REST catalog..."
kubectl -n iceberg-janitor wait --for=condition=ready pod -l app=rest-catalog --timeout=120s

# Wait for Knative service to be ready
echo "==> Waiting for Knative service iceberg-janitor..."
kubectl -n iceberg-janitor wait --for=condition=Ready ksvc/iceberg-janitor --timeout=300s

echo ""
echo "==> Dev stack is ready!"
echo ""
echo "Services:"
echo "  MinIO API:      kubectl -n iceberg-janitor port-forward svc/minio 9000:9000"
echo "  MinIO Console:  kubectl -n iceberg-janitor port-forward svc/minio 9001:9001"
echo "  REST Catalog:   kubectl -n iceberg-janitor port-forward svc/rest-catalog 8181:8181"
echo "  Kafka:          kubectl -n iceberg-janitor port-forward svc/kafka 9092:9092"
echo ""
echo "Knative service URL:"
KSVC_URL=$(kubectl -n iceberg-janitor get ksvc iceberg-janitor -o jsonpath='{.status.url}' 2>/dev/null || echo "<pending>")
echo "  ${KSVC_URL}"
echo ""
echo "Test the Knative service:"
echo "  curl -X POST \${KSVC_URL} -H 'Content-Type: application/json' -d '{\"trigger\": \"manual\", \"scope\": \"all_tables\"}'"
echo "  curl \${KSVC_URL}/health"
echo ""
echo "Generate test data:"
echo "  kubectl apply -f ${PROJECT_DIR}/manifests/dev/data-generator.yaml"
echo ""
echo "Run janitor manually:"
echo "  janitor analyze s3://warehouse/events_db/events"
echo "  janitor maintain events_db.events --catalog-uri http://localhost:8181 --warehouse s3://warehouse/ --s3-endpoint http://localhost:9000 --dry-run"
