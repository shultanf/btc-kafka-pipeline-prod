import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaConsumer
from botocore.exceptions import BotoCoreError, ClientError
import boto3
import io
from collections import defaultdict

# -------------------------
# CONFIG
# -------------------------
KAFKA_BROKERS = ["localhost:9092"]   # your broker(s)
TOPIC = "crypto-prices"
GROUP_ID = "s3-uploader-consumer"

AWS_REGION = "ap-southeast-1"
S3_BUCKET = "your-s3-bucket"
S3_PREFIX = "crypto_batches/"

FLUSH_DELAY = 60  # seconds grace for late arrivals

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("KafkaS3Consumer")


class KafkaS3TimeWindowConsumer:
    def __init__(self, topic, s3_bucket, s3_prefix):
        self.topic = topic
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

        # Kafka consumer
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=GROUP_ID,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
        )

        # AWS S3
        self.s3 = boto3.client("s3", region_name=AWS_REGION)

        # Nested dict: {window_start: {symbol: [messages...]}}
        self.windows = defaultdict(lambda: defaultdict(list))
        """
        {
                window_start_dt1: 
                {
                        "BTC": [msg, msg, ...],
                        "ETH": [msg, ...],
                        ...
                },
                window_start_dt2:
                {
                        ...
                }
        }

        how to index -> self.window[window_start_dt][slug]
        
        """

        # Graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _get_window_start(self, ts: datetime) -> datetime:
        """
        Align a timestamp to its 15-minute window start.
        Example: 12:07 → 12:00; 12:19 → 12:15
        """
        floored_minute = ts.minute - (ts.minute % 15)
        return ts.replace(minute=floored_minute, second=0, microsecond=0)

    def _upload_to_s3(self, messages, window_start, symbol):
        if not messages:
            return

        key = f"{self.s3_prefix}{symbol}/{window_start.strftime('%Y%m%dT%H%M')}.jsonl"
        buffer = io.StringIO()
        for msg in messages:
            buffer.write(json.dumps(msg) + "\n")
        buffer.seek(0)

        try:
            self.s3.upload_fileobj(buffer, self.s3_bucket, key)
            logger.info(
                f"✅ Uploaded {len(messages)} {symbol} msgs for window {window_start} "
                f"to s3://{self.s3_bucket}/{key}"
            )
        except (BotoCoreError, ClientError) as e:
            logger.error(f"❌ Failed to upload {symbol} window {window_start}: {e}", exc_info=True)
        finally:
            buffer.close()

    def _flush_expired_windows(self):
        """
        Flush windows that are older than current time - FLUSH_DELAY.
        Ensures late messages are included, but old windows eventually close.
        """
        now = datetime.now(timezone.utc)
        expired = []
        for win_start, symbol_batches in self.windows.items():
            window_end = win_start + timedelta(minutes=15)
            if now > window_end - timedelta(seconds=FLUSH_DELAY):
                for symbol, msgs in symbol_batches.items():
                    self._upload_to_s3(msgs, win_start, symbol)
                expired.append(win_start)

        for win in expired:
            del self.windows[win]

    def _handle_shutdown(self, signum, frame):
        logger.info("🛑 Shutdown signal received, flushing all remaining windows...")
        for win_start, symbol_batches in self.windows.items():
            for symbol, msgs in symbol_batches.items():
                self._upload_to_s3(msgs, win_start, symbol)
        self.consumer.close()
        sys.exit(0)

    def run(self):
        logger.info("🚀 Kafka consumer started (time-window + per-symbol batching). Waiting for messages...")
        try:
            for message in self.consumer:
                data = message.value
                # Example: {"symbol": "BTC", "price": ..., "timestamp": "2025-08-22T10:07:00Z"}
                ts = datetime.fromisoformat(data["timestamp"]).astimezone(timezone.utc)
                symbol = data.get("symbol")
                if not symbol:
                    logger.warning(f"Skipping message with no symbol: {data}")
                    continue

                window_start = self._get_window_start(ts)
                self.windows[window_start][symbol].append(data) # Append message to the general list (self.window)

                # Periodically check for expired windows to flush
                self._flush_expired_windows()

        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}", exc_info=True)
        finally:
            logger.info("Flushing all windows before exit...")
            for win_start, symbol_batches in self.windows.items():
                for symbol, msgs in symbol_batches.items():
                    self._upload_to_s3(msgs, win_start, symbol)
            self.consumer.close()


if __name__ == "__main__":
    consumer = KafkaS3TimeWindowConsumer(
        topic=TOPIC,
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
    )
    consumer.run()
