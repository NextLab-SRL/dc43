# syntax=docker/dockerfile:1
FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DC43_CONTRACT_STORE=/contracts

WORKDIR /app

# Install dc43 service packages from the repository.
COPY packages/dc43-core /tmp/dc43-core
COPY packages/dc43-service-clients /tmp/dc43-service-clients
COPY packages/dc43-service-backends /tmp/dc43-service-backends

RUN pip install --no-cache-dir /tmp/dc43-core \
    && pip install --no-cache-dir /tmp/dc43-service-clients \
    && pip install --no-cache-dir "/tmp/dc43-service-backends[http]" \
    && rm -rf /tmp/dc43-core /tmp/dc43-service-clients /tmp/dc43-service-backends

EXPOSE 8001

CMD ["uvicorn", "dc43_service_backends.webapp:app", "--host", "0.0.0.0", "--port", "8001"]
