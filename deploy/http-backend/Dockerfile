# syntax=docker/dockerfile:1
FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

# Install dc43 service packages from the repository.
COPY packages/dc43-core /tmp/dc43-core
COPY packages/dc43-service-clients /tmp/dc43-service-clients
COPY packages/dc43-service-backends /tmp/dc43-service-backends

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install /tmp/dc43-core \
    && /opt/venv/bin/pip install "/tmp/dc43-service-clients[http]" \
    && /opt/venv/bin/pip install "/tmp/dc43-service-backends[http]" \
    && /opt/venv/bin/pip install "uvicorn[standard]>=0.24"


FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="dc43-http-backend" \
      org.opencontainers.image.description="Data Contracts Governance Backend API" \
      org.opencontainers.image.source="https://github.com/NextLab-SRL/dc43"

ENV PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    DC43_CONTRACT_STORE=/contracts

# Create non-root user
RUN groupadd -r dc43 && useradd -r -g dc43 dc43 \
    && mkdir -p /contracts \
    && chown -R dc43:dc43 /contracts

WORKDIR /app
COPY --from=builder --chown=dc43:dc43 /opt/venv /opt/venv

USER dc43

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8001/openapi.json')" || exit 1

CMD ["/opt/venv/bin/uvicorn", "dc43_service_backends.webapp:app", "--host", "0.0.0.0", "--port", "8001"]
