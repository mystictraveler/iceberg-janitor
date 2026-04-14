#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "==> Tearing down dev stack..."
kubectl delete -k "${PROJECT_DIR}/manifests/dev" --ignore-not-found

echo "==> Dev stack removed."
