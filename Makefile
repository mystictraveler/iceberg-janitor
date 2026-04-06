.PHONY: install dev test lint format docker-build kind-setup knative-setup kind-setup-knative dev-up dev-down clean

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

knative-setup:
	@echo "==> Installing Knative into cluster..."
	@KNATIVE_SERVING_VERSION=v1.17.0; \
	KNATIVE_EVENTING_VERSION=v1.17.0; \
	KNATIVE_KAFKA_VERSION=v1.17.0; \
	echo "==> Installing Knative Serving CRDs + core ($${KNATIVE_SERVING_VERSION})..."; \
	kubectl apply -f "https://github.com/knative/serving/releases/download/knative-$${KNATIVE_SERVING_VERSION}/serving-crds.yaml"; \
	kubectl apply -f "https://github.com/knative/serving/releases/download/knative-$${KNATIVE_SERVING_VERSION}/serving-core.yaml"; \
	echo "==> Installing Kourier networking layer..."; \
	kubectl apply -f "https://github.com/knative/net-kourier/releases/download/knative-$${KNATIVE_SERVING_VERSION}/kourier.yaml"; \
	kubectl patch configmap/config-network --namespace knative-serving --type merge \
		--patch '{"data":{"ingress-class":"kourier.ingress.networking.knative.dev"}}'; \
	echo "==> Configuring default domain..."; \
	kubectl apply -f "https://github.com/knative/serving/releases/download/knative-$${KNATIVE_SERVING_VERSION}/serving-default-domain.yaml"; \
	echo "==> Installing Knative Eventing CRDs + core ($${KNATIVE_EVENTING_VERSION})..."; \
	kubectl apply -f "https://github.com/knative/eventing/releases/download/knative-$${KNATIVE_EVENTING_VERSION}/eventing-crds.yaml"; \
	kubectl apply -f "https://github.com/knative/eventing/releases/download/knative-$${KNATIVE_EVENTING_VERSION}/eventing-core.yaml"; \
	echo "==> Installing Knative Kafka source controller ($${KNATIVE_KAFKA_VERSION})..."; \
	kubectl apply -f "https://github.com/knative-extensions/eventing-kafka-broker/releases/download/knative-$${KNATIVE_KAFKA_VERSION}/eventing-kafka-controller.yaml"; \
	kubectl apply -f "https://github.com/knative-extensions/eventing-kafka-broker/releases/download/knative-$${KNATIVE_KAFKA_VERSION}/eventing-kafka-source.yaml"; \
	echo "==> Waiting for Knative Serving pods..."; \
	kubectl wait --for=condition=ready pod --all -n knative-serving --timeout=300s; \
	echo "==> Waiting for Knative Eventing pods..."; \
	kubectl wait --for=condition=ready pod --all -n knative-eventing --timeout=300s; \
	echo "==> Knative installation complete!"

kind-setup-knative: kind-setup knative-setup

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
