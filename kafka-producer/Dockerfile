# Use the official slim Python base image
FROM docker.arvancloud.ir/python:3.11-slim

# Create application directory
WORKDIR /app

# Install build deps for confluent-kafka if needed
# confluent-kafka has a binary wheel for many platforms; keep install minimal
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ libssl-dev libsasl2-dev librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home appuser
USER appuser
ENV HOME=/home/appuser

# Copy source
COPY --chown=appuser:appuser producer.py /app/producer.py
COPY --chown=appuser:appuser requirements.txt /app/requirements.txt

# Install Python deps as non-root user (no-cache)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Expose nothing (producer is outbound), but set HEALTHCHECK if you want
# HEALTHCHECK could exercise schema-registry or kafka; omitted for simplicity.

ENTRYPOINT ["python", "producer.py"]
