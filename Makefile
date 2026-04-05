.PHONY: install dev test lint format docker-build kind-setup dev-up dev-down clean

CLUSTER_NAME ?= iceberg-janitor
IMAGE_NAME ?= iceberg-janitor
IMAGE_TAG ?= latest

# Python
install:
	pip install -e .

dev:
	pip install -e ".[dev,api]"

test:
	pytest -v --tb=short

lint:
	ruff check src/ tests/
	mypy src/

format:
	ruff format src/ tests/
	ruff check --fix src/ tests/

# Docker
docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) -f docker/Dockerfile .

docker-load: docker-build
	kind load docker-image $(IMAGE_NAME):$(IMAGE_TAG) --name $(CLUSTER_NAME)

# Kubernetes
kind-setup:
	./scripts/kind-setup.sh $(CLUSTER_NAME)

dev-up:
	./scripts/dev-stack-up.sh

dev-down:
	./scripts/dev-stack-down.sh

generate-data:
	kubectl apply -f manifests/dev/data-generator.yaml

# Flink
flink-build:
	cd flink-jobs && mvn clean package -DskipTests

# Cleanup
clean:
	rm -rf dist/ build/ *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
