import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from time import sleep
import json
#from json import dumps, loads
import time
import logging
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
import os
from pathlib import Path
from s3_client import S3Uploader

"""
Program Flow
Received data -> Batch data -> Send to S3 bucket

- Receive data from kafka producer
- Batch data in 15 records
- Use the function made to send the batch data to S3 bucket
"""

# Loads variables from .env
#env_path = Path(__file__).resolve().parent
#load_dotenv(dotenv_path=env_path)
load_dotenv('/app/.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('kafka-consumer')
logging.info("Starting kafka consumer 1 . . .")

class resilientConsumer:
    def __init__(self, bootstrap_server, topic_name, bucket_name):
        # Kafka server
        self.topic_name=topic_name
        self.bootstrap_servers=bootstrap_server
        
        # Batching
        self.batch = []
        self.batch_size = 15
        # Error handling
        self.max_retries = 3
        self.max_delays = 5
        # S3 upload func
        self.bucket_name = bucket_name
        self.s3_uploader = S3Uploader(bucket_name=bucket_name) # S3 uploader

        self.consumer = self._create_consumer()

    def __iter__(self):
        """Make the consumer iterable by delegating to the Kafka consumer"""
        return self.consumer.__iter__()
    
    def safe_deserializer(self, message):
        try:
            return json.loads(message.decode('utf-8')) if message else None
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            logger.error(f"Skipping bad message (raw byte: {message!r}).")
            return None  # Continue processing
    
    def _create_consumer(self):
        return KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')), # for every value received (json:str), convert to python object
            auto_offset_reset='latest',
            group_id="my-group-1",
            enable_auto_commit=False
        )
    def to_bucket(self, message):
        logger.info("Message received.",extra={"message_value":str(message)})
        try:
            # Batch data
            self.batch.append(message)

            # Create key
            utc_now = str(datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S%f")[:-3])
            key = f"bitcoin-hist-data/btc_kafka-consumer_{utc_now}.json"

            # Upload batch to S3
            if len(self.batch) == self.batch_size:
                logger.info(f"Current batch len is ({len(self.batch)}/{self.batch_size}). Uploading to S3 . . .")
                success = self.s3_uploader.upload_batch(self.batch, key=key)

                if success:
                    self.batch = []
                    logger.info(f"Batch upload complete.")
                    return True
                else:
                    logger.error("Batch upload failed.")

                    self.batch = []
                    return False
            else:
                logger.error(f"Current batch len is ({len(self.batch)}/{self.batch_size}). Waiting for more message . . .")
            

        # Logger.error when there's exception
        except json.JSONDecodeError as e:
            logger.error(f"Invalid message format: {e}")
        except KeyError as e:
            logger.error(f"Missing required field in message: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in to_bucket: {e}")

        return False

def main():
    # Create consumer object
    consumer = resilientConsumer(bootstrap_server=[f"{os.getenv('PORT_C_KAFKA')}:{os.getenv('PORT_CLIENT')}"],
                                 topic_name=os.getenv('TOPIC_NAME'),
                                 bucket_name=os.getenv('BUCKET_NAME'))
    
    # Send data to S3 bucket
    for message in consumer:
        logger.info(f"message received: {message.value}")
        consumer.to_bucket(message=message.value)

if __name__ == "__main__":
    main()

        



