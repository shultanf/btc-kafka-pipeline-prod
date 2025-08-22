import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from time import sleep
import json
import time
import logging
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
import os
from pathlib import Path
from s3_client import S3Uploader
import io
import pyarrow

load_dotenv('/app/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('kafka-consumer-1')
logging.info("Starting kafka consumer 1 . . .")

class resilientConsumer:
    def __init__(self, host, port, topic_name, bucket_name):
        # Kafka server
        self.topic_name=topic_name
        self.host=host
        self.port=port
        # Batching
        self.batch = []
        self.batch_size = 15
        # Error handling
        self.max_retries = 3
        self.max_delays = 5
        # S3 upload func
        self.bucket_name = bucket_name
        self.s3_uploader = S3Uploader(bucket_name=bucket_name) # S3 uploader
        # Create consumer object
        self.consumer = self._create_consumer()

    def __iter__(self):
        return self.consumer.__iter__()
    
    def safeDeserialization(self, message):
        try:
            return json.loads(message.decode('utf-8')) if message else None
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            logger.error(f"Skipping bad message (raw byte: {message!r}).")
            return None 
    
    def _create_consumer(self):
        return KafkaConsumer(
            self.topic_name,
            bootstrap_servers=[f"{self.host}:{self.port}"],
            value_deserializer=self.safeDeserialization, 
            auto_offset_reset='latest',
            group_id="my-group-1",
            enable_auto_commit=False
        )
    def isTimetoFlush(self):
        if not hasattr(self, "last_flush_time"):
            self.last_flush_time = time.monotonic()
            return False
        now = time.monotonic()
        return (now - self.last_flush_time) >= 300

    
    def toBucket(self, message: dict):
        # Only flush existing batch
        if len(self.batch) >= self.batch_size or self.isTimetoFlush():
            if self.batch:  # flush only if not empty
                df = pd.DataFrame(self.batch)
                buffer = io.BytesIO()
                df.to_parquet(buffer, engine="pyarrow", index=False)
                buffer.seek(0)

                utc_now = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S%f")[:-3]
                key = f"batch_len={len(self.batch)}_{utc_now}.parquet"

                self.s3_uploader.upload_batch(buffer, key=key)
                logger.info(f"Flushed {len(self.batch)} messages to {key}")

                self.batch = []
                self.last_flush_time = time.monotonic()

        # Always append *after* flushing
        self.batch.append(message)
        if len(self.batch) >= self.batch_size:
            df = pd.DataFrame(self.batch)
            buffer = io.BytesIO()
            df.to_parquet(buffer, engine="pyarrow", index=False)
            buffer.seek(0)

            utc_now = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S%f")[:-3]
            key = f"batch_len={len(self.batch)}_{utc_now}.parquet"

            self.s3_uploader.upload_batch(buffer, key=key)
            logger.info(f"Flushed {len(self.batch)} messages to {key}")

            self.batch = []
            self.last_flush_time = time.monotonic()

        logger.info(f"Batch status: {len(self.batch)}/{self.batch_size}")


def main():
    # Create consumer object
    consumer = resilientConsumer(host=os.environ['HOST'],
                                 port=os.environ['PORT'],
                                 topic_name=os.environ['TOPIC_NAME'],
                                 bucket_name=os.environ['BUCKET_NAME'])
    # Send data to S3 bucket
    for message in consumer:
        logger.info(f"message received: {message.value}")
        consumer.toBucket(message=message.value)

if __name__ == "__main__":
    main()



