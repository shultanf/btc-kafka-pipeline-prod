FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y netcat-openbsd && \
    rm -rf /var/lib/apt/lists/* 

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY /src ./src/
RUN chmod +x /usr/local/bin/wait-broker.sh

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=5 \
    CMD nc -z kafka 9092 || exit 1








