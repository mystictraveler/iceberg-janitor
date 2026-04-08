.PHONY: install dev test lint format docker-build kind-setup knative-setup kind-setup-knative dev-up dev-down clean go-build go-test go-vet go-tidy go-clean mvp-up mvp-down mvp-seed mvp-seed-local mvp-analyze mvp-analyze-local mvp-discover

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

# Go (sibling implementation under go/)
go-build:
	cd go && go build ./...

go-test:
	cd go && go test ./...

go-vet:
	cd go && go vet ./...

go-tidy:
	cd go && go mod tidy

go-clean:
	cd go && rm -rf bin/

# === Go MVP test loop ===
# Two paths to test the MVP:
#   (a) docker MinIO + S3 — closer to production, requires Docker running
#   (b) local fileblob    — no infrastructure, just a /tmp directory
# Both use the same seed binary and the same Go janitor.

MVP_WAREHOUSE_LOCAL ?= file:///tmp/janitor-mvp
MVP_WAREHOUSE_S3    ?= s3://warehouse?endpoint=http://localhost:9000&s3ForcePathStyle=true&region=us-east-1
MVP_NAMESPACE       ?= mvp
MVP_TABLE           ?= events
MVP_NUM_BATCHES     ?= 100
MVP_ROWS_PER_BATCH  ?= 200

mvp-up:
	docker compose -f go/test/mvp/docker-compose.yml up -d
	@echo "MinIO console: http://localhost:9001  (minioadmin/minioadmin)"

mvp-down:
	docker compose -f go/test/mvp/docker-compose.yml down -v

# Seed against MinIO (requires mvp-up first).
mvp-seed:
	cd go && JANITOR_WAREHOUSE_URL='s3://warehouse' \
		S3_ENDPOINT=http://localhost:9000 \
		S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin \
		NAMESPACE=$(MVP_NAMESPACE) TABLE=$(MVP_TABLE) \
		NUM_BATCHES=$(MVP_NUM_BATCHES) ROWS_PER_BATCH=$(MVP_ROWS_PER_BATCH) \
		CATALOG_DB=/tmp/janitor-mvp-catalog.db \
		go run -tags bench ./test/bench/seed

# Seed against the local filesystem (no Docker).
mvp-seed-local:
	rm -rf /tmp/janitor-mvp /tmp/janitor-mvp-catalog.db
	cd go && JANITOR_WAREHOUSE_URL='file:///tmp/janitor-mvp' \
		NAMESPACE=$(MVP_NAMESPACE) TABLE=$(MVP_TABLE) \
		NUM_BATCHES=$(MVP_NUM_BATCHES) ROWS_PER_BATCH=$(MVP_ROWS_PER_BATCH) \
		CATALOG_DB=/tmp/janitor-mvp-catalog.db \
		go run -tags bench ./test/bench/seed

mvp-discover:
	cd go && JANITOR_WAREHOUSE_URL='$(MVP_WAREHOUSE_S3)' \
		AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
		S3_ENDPOINT=http://localhost:9000 S3_REGION=us-east-1 \
		go run ./cmd/janitor-cli discover

mvp-discover-local:
	cd go && JANITOR_WAREHOUSE_URL='$(MVP_WAREHOUSE_LOCAL)' \
		go run ./cmd/janitor-cli discover

mvp-analyze:
	cd go && JANITOR_WAREHOUSE_URL='$(MVP_WAREHOUSE_S3)' \
		AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
		S3_ENDPOINT=http://localhost:9000 S3_REGION=us-east-1 \
		go run ./cmd/janitor-cli analyze $(MVP_NAMESPACE).db/$(MVP_TABLE)

mvp-analyze-local:
	cd go && JANITOR_WAREHOUSE_URL='$(MVP_WAREHOUSE_LOCAL)' \
		go run ./cmd/janitor-cli analyze $(MVP_NAMESPACE).db/$(MVP_TABLE)

# DuckDB query benchmark — runs after seed (and after compaction).
# Reads the same Iceberg table the Go janitor maintains, via DuckDB's
# iceberg extension. Round-trip verification: an independent query
# engine MUST see the same data the Go writer wrote and the Go janitor
# rewrote.
#
# DuckDB refuses to "guess" the current metadata version without an
# explicit path (it considers max-version scan unsafe under multi-writer
# concurrency). For local fileblob we resolve the path with `ls`. For
# MinIO we set unsafe_enable_version_guessing because the MVP test loop
# is single-writer and we want a flag-free experience for the operator.

mvp-query-local:
	@meta=$$(ls -1 /tmp/janitor-mvp/mvp.db/events/metadata/*.metadata.json | sort | tail -1) && \
	echo "metadata: $$meta" && \
	duckdb -c "INSTALL iceberg; LOAD iceberg; \
		SELECT count(*) AS row_count, count(DISTINCT user_id) AS distinct_users, \
		       count(DISTINCT event_type) AS event_types \
		FROM iceberg_scan('$$meta');"

# Round-trip query against the MinIO warehouse via DuckDB's httpfs +
# iceberg extensions. Requires MinIO to be running (`make mvp-up`) and
# a seeded table (`make mvp-seed`).
mvp-query:
	@duckdb -c "INSTALL httpfs; LOAD httpfs; \
		INSTALL iceberg; LOAD iceberg; \
		CREATE OR REPLACE SECRET minio_secret ( \
			TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin', \
			ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false); \
		SET unsafe_enable_version_guessing = true; \
		SELECT count(*) AS row_count, \
		       count(DISTINCT user_id) AS distinct_users, \
		       count(DISTINCT event_type) AS event_types \
		FROM iceberg_scan('s3://warehouse/$(MVP_NAMESPACE).db/$(MVP_TABLE)');"

# === TPC-DS streaming benchmark (with-janitor vs without-janitor) ===
# Drives two parallel warehouses, streams identical TPC-DS micro-batches
# into both, periodically runs janitor compact on the with-janitor side,
# runs the 10 canonical TPC-DS queries via DuckDB at intervals, records
# results to CSV, prints a comparison report.
#
# Defaults: 5 minutes, local fileblob, no Docker. For AWS, set
# WAREHOUSE_BASE=s3://your-bucket and provide AWS credentials.

bench-tpcds:
	@./go/test/bench/bench-tpcds.sh

bench-tpcds-quick:
	@DURATION_SECONDS=60 QUERY_INTERVAL_SECONDS=15 MAINTENANCE_INTERVAL_SECONDS=20 \
		./go/test/bench/bench-tpcds.sh

bench-tpcds-aws:
	@WAREHOUSE_BASE=$${WAREHOUSE_BASE:?set WAREHOUSE_BASE to s3://your-bucket} \
		DURATION_SECONDS=$${DURATION_SECONDS:-1800} \
		QUERY_INTERVAL_SECONDS=$${QUERY_INTERVAL_SECONDS:-120} \
		MAINTENANCE_INTERVAL_SECONDS=$${MAINTENANCE_INTERVAL_SECONDS:-180} \
		./go/test/bench/bench-tpcds.sh

# Cleanup
clean:
	rm -rf dist/ build/ *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
