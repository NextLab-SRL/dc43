# syntax=docker/dockerfile:1
FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DC43_CONTRACTS_APP_BACKEND_MODE=remote

WORKDIR /app

# Install dc43 packages from the repository to keep contracts metadata in sync with the
# backend services.
COPY packages/dc43-core /tmp/dc43-core
COPY packages/dc43-service-clients /tmp/dc43-service-clients
COPY packages/dc43-service-backends /tmp/dc43-service-backends
COPY packages/dc43-contracts-app /tmp/dc43-contracts-app

RUN pip install --no-cache-dir /tmp/dc43-core \
    && pip install --no-cache-dir /tmp/dc43-service-clients \
    && pip install --no-cache-dir "/tmp/dc43-service-backends[http]" \
    && pip install --no-cache-dir /tmp/dc43-contracts-app \
    && pip install --no-cache-dir "uvicorn[standard]>=0.24" \
    && rm -rf /tmp/dc43-core /tmp/dc43-service-clients /tmp/dc43-service-backends /tmp/dc43-contracts-app

EXPOSE 8000

CMD ["uvicorn", "dc43_contracts_app.server:app", "--host", "0.0.0.0", "--port", "8000"]
