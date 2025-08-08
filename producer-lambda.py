import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from time import sleep
import json
from json import dumps
import time
import logging
from datetime import datetime, timezone
import requests
from requests.exceptions import RequestException, HTTPError, Timeout
from dotenv import load_dotenv
import os
from pathlib import Path

time.sleep(15)

# Loads variables from .env
# env_path = Path(__file__).resolve().parent
# load_dotenv(dotenv_path=env_path)
load_dotenv('/app/.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('kafka-producer')
logging.info("Starting kafka producer . . .")

class retryProducer:
    def __init__(self, bootstrap_servers, topic_name):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.max_retries = 3
        self.max_delays = 5
        try:
            self.producer = self._create_producer()
            logger.info(f"Successfully created kafka producer instance. bootstrap_servers={self.bootstrap_servers} topic={self.topic_name}")
        except Exception as e:
            logger.error(f"Cannot connect to kafka server. bootstrap_servers={self.bootstrap_servers} topic={self.topic_name}", exc_info=e)
    
    
    def _create_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries = 3
            # max_in_flight_requests_per_connection=1,  # Prevent message reordering
            # request_timeout_ms=30000,  # Timeout for requests (30s)
            # reconnect_backoff_ms=1000  # Delay between reconnection attempts
            )

    def get_btc(self):
        # Variables needed for API requests
        url = f"{os.getenv('API_BASE_URL')}assets/bitcoin/history"
        headers = {"Authorization": f"Bearer {os.getenv('API_KEY')}"}
        
        # Variables needed for API parameter
        end_date = int(datetime.now(timezone.utc).timestamp()*1000) # Current timestamp
        start_date = end_date - (15*60*1000) # timestamp 
        utc_now = datetime.now(timezone.utc) # time now (UTC)

        # Parameter dict
        params = {
            "interval":"m1",
            "start":start_date,
            "end":end_date
        }

        # Make API requests
        try:
            response = requests.get(url=url, headers=headers, params=params)
            response.raise_for_status()

            if response:
                data = response.json()["data"]

                return data
        except Timeout as e:
            logger.error("Requests timed out.", exc_info=e)
            return False
        except HTTPError as e:
            logger.error(f"HTTP error: {e.response.status_code}", exc_info=e)
            return False
        except ConnectionError as e:
            logger.error(f"Requests connection error.",exc_info=e)
            return False
        except RequestException as e:
            logger.error(f"General requests error.", exc_info=e)
            return False
        else:
            logger.warning(f"Something happened when making requests, response: {response.json()}")
            return False
    
    def send_message(self, message):
        attempt = 0
        if isinstance(message, str):
            message = json.loads(message)
        if isinstance(message, dict):
            pass
        else:
            logger.error("Message is neither type: str or type: dict")
            raise ValueError("Message is neither type: str nor type: dict")
        while attempt <= self.max_retries:
            try:
                success = self.producer.send(self.topic_name,value=message)
                if success:
                    metadata = success.get(timeout=10)
                    logger.info(
                        f"Message delivered to {metadata.topic} [partition {metadata.partition}].\nmsg_content:{str(message)}")
                    self.producer.flush()
                    return True
            except Exception as e:
                attempt += 1
                logger.error(
                    f"Attempt {attempt}/{self.max_retries} failed.",
                    exc_info=e
                    #extra={"topic":metadata.topic, "message":{str(message)}}
                           )
                if attempt > self.max_retries:
                    try:
                        self.producer = self._create_producer() # Reconnect to server by recreating producer object
                    # If can't connect to broker -> logger.critical()
                    except Exception as e:
                        logger.critical(
                            "Cannot connect to Kafka Broker server.",
                            exc_info=e
                            )
                        return False
                else:
                    time.sleep(1)

    def close(self):
        self.producer.flush(timeout=10)
        self.producer.close()

def main():
    # Initiate connection to kafka broker
    producer = retryProducer(bootstrap_servers=[f"{os.getenv('PORT_C_KAFKA')}:{os.getenv('PORT_CLIENT')}"], 
                             topic_name=os.getenv('TOPIC_NAME'))

    # Fetch btc data from API then send message to kafka consumer
    try:
        # Loop the API request and message sending process (every 15m)
        while True:
            try:
                btc = producer.get_btc()
                len_btc = (len(btc))

                # Sending message to consumer per record
                if len_btc == 15:
                    for i in range(len(btc)):
                        logger.info("Sending message . . .")
                        logger.info(f"Message value: {btc[i]}")
                        logger.info(f"Message type: {type(btc[i])}")
                        producer.send_message(btc[i])
                else:
                    raise ValueError(f"Bitcoin API needed to fetch 15 records. Records received: {len_btc}.")
            except Exception as e:
                logger.error(f"Error occurred: {e} Sending request to API again in 15 minute.")

            # Request for another batch in 15m
            time.sleep(901)

    # Close producer when KeyboardInterrupt
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.close()

# Run main()
if __name__ == "__main__":
    main()

            





                
        