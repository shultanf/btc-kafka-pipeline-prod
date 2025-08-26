import json
import logging
import signal
import sys
import time
import threading
import io
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from confluent_kafka import Consumer, KafkaError, KafkaException
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from collections import defaultdict

load_dotenv('/app/.env')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("consumer-1")

# Consumer Config
CONSUMER_CONF = {
    'bootstrap.servers':f"{os.environ['HOST']}:{os.environ['PORT']}",
    'group.id':os.environ['GROUP_ID'],
    'auto.offset.reset':'earliest'
}
TOPIC = os.environ['TOPIC']

# S3 Bucket Config
AWS_REGION = os.environ['AWS_REGION']
BUCKET = os.environ['BUCKET']
S3_PREFIX = ""

# Batching Config
WINDOW_MINUTES = 15 # In minutes
FLUSH_DELAY = 60 # In seconds
FORCE_FLUSH_INTERVAL = 900 # In seconds


class kafkaConsumerToS3:
    def __init__(self):
        # Confluent consumer
        self.consumer = Consumer(CONSUMER_CONF)
        self.consumer.subscribe([TOPIC])

        # AWS S3
        self.s3 = boto3.client("s3", region_name=AWS_REGION)

        # Windows
        self.windows = defaultdict(lambda: defaultdict(list)) # self.windows[floored_ts][slug] = [msg, msg, ...]
        self.running = True

        # Periodic flush
        self.lock = threading.Lock()

        self.flush_thread = threading.Thread(target=self._periodic_flush, daemon=True) # Runs in the background
        self.flush_thread.start()

        # Flush on shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _get_window_start(self, ts:datetime) -> datetime:
        floored_ts = ts.minute - (ts.minute % WINDOW_MINUTES)
        return ts.replace(minute=floored_ts, second=0, microsecond=0)
    
    def _upload_to_s3(self, messages, window_start, symbol):
        if not messages:
            return
        key = f"{symbol}/{window_start.strftime('%Y%m%dT%H%M')}.jsonl"
        buffer = io.StringIO()

        for msg in messages:
            buffer.write(json.dumps(msg) + "\n")
        buffer.seek(0)
        try:
            self.s3.upload_fileobj(buffer, BUCKET, key)
            logger.info(
                f"Uploaded {len(messages)} {symbol} msgs with timestamp {window_start} "
                f"to s3://{BUCKET}/{key}"
            )
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed upload {symbol} timestamp {window_start}: {e}", exc_info=True)
        finally:
            buffer.close()

    def _flush_expire_windows(self, force=False):
        """
        Iterate every window_start, if now > window_end then flush that window.
        After flushing delete flushed window from dict.
        """
        now = datetime.now(timezone.utc)
        expired = []
        
        with self.lock:
            for window_start, symbol_batches in self.windows.items():
                window_end = window_start + timedelta(minutes=WINDOW_MINUTES)
                if force or now > window_end + timedelta(seconds=FLUSH_DELAY):
                    for symbol, msgs in symbol_batches.items():
                        self._upload_to_s3(msgs, window_start, symbol)
                    expired.append(window_start)

            for win in expired:
                del self.windows[win]
        logger.info("Expired windows flushed.")

    def _periodic_flush(self):
        while self.running:
            time.sleep(FORCE_FLUSH_INTERVAL)
            logger.info("Periodic flush triggered.")

            self._flush_expire_windows(force=True)
        
    def _shutdown(self, signum, frame):
        logger.info("Shutdown received. Flushing all windows.")
        self.running = False
        self._flush_expire_windows()
        self.consumer.close()
        self.running = False

    def consumer_metadata(self):
        metadata = self.consumer.list_topics(timeout=10)
        logger.info("Connected to cluster:")
        logger.info(f"Cluster ID: {metadata.cluster_id}")
        logger.info(f"Brokers: {metadata.brokers}")

        logger.info("\nAvailable topics:")
        for t in metadata.topics.keys():
            logger.info(f" - {t}")

    def run(self):
        try:
            while self.running:
                try:
                    msg = self.consumer.poll(1.0)
                    self.consumer_metadata() # Check metadata
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            raise KafkaException(msg.error()) # Raised exception will be catched by the "except:" block.

                    try:
                        record = json.loads(msg.value().decode('utf-8'))
                    except Exception as e:
                        logger.error("Failed to deserialize message.", exc_info=e)

                    ts = datetime.fromisoformat(record["date"])
                    ts_floored = self._get_window_start(ts)
                    symbol = record["slug"]

                    self.windows[ts_floored][symbol].append(record)

                    self._flush_expire_windows()

                except Exception as e:
                    logger.error("Error while consuming messages", exc_info=e)

        except Exception as e:
            logger.error("Fata error in consumer loop.", exc_info=e)
        finally:
            logger.info("Closing consumer gracefully...")
            self._flush_expire_windows(force=True)
            self.consumer.close()

if __name__ == "__main__":
    consumer = kafkaConsumerToS3()
    consumer.run()
