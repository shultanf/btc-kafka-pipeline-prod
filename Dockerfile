FROM python:3.11-slim
# Create new dir on the container
WORKDIR /app

RUN apt-get update && apt-get install -y netcat-openbsd && \
    rm -rf /var/lib/apt/lists/* 

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY /src ./src/
COPY wait-broker.sh /usr/local/bin/wait-broker.sh
RUN chmod +x /usr/local/bin/wait-broker.sh

# Best practice for environment variables (don't copy .env)
# ENV vars should be passed via docker-compose or runtime

# Health check (adjust as needed)
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=5 \
    CMD nc -z kafka 9092 || exit 1
    #CMD python -c "from kafka import KafkaConsumer; KafkaConsumer(bootstrap_servers=['kafka:9092'])"
    #CMD python -c "from confluent_kafka import Consumer; Consumer({'bootstrap.servers':'kafka:9092})"

# Use ENTRYPOINT for the wait script and CMD for the main process
#ENTRYPOINT ["wait-consumer.sh"]
#CMD ["python", "src/s3_client.py", "src/consumer.py", "src/producer.py"] 







