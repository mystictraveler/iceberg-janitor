FROM python:3.11-slim AS builder

WORKDIR /app
COPY pyproject.toml .
COPY src/ src/

RUN pip install --no-cache-dir .

FROM python:3.11-slim

WORKDIR /app
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/janitor /usr/local/bin/janitor
COPY scripts/generate-data.py /app/scripts/generate-data.py

# Default config location for K8s ConfigMap mount
ENV JANITOR_CONFIG_PATH=/etc/janitor/config.yaml

ENTRYPOINT ["janitor"]
CMD ["--help"]
