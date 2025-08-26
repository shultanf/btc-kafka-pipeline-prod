import json
import logging
import signal
import sys
import time
import threading
import io
from datetime import datetime, timezone, timedelta
from kafka import KafkaConsumer
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from collections import defaultdict

# ---------------- CONFIG ----------------
KAFKA_BROKERS = ["localhost:9092"]
TOPIC = "crypto-prices"
GROUP_ID = "hybrid-consumer"

AWS_REGION = "ap-southeast-1"
S3_BUCKET = "your-s3-bucket"
S3_PREFIX = "crypto_batches/"

WINDOW_MINUTES = 15         # event-time window size
FLUSH_DELAY = 60            # grace period for late arrivals
FORCE_FLUSH_INTERVAL = 300  # flush every 5 mins regardless of new data

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("KafkaS3Hybrid")

class KafkaS3HybridConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=GROUP_ID,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
        )
        self.s3 = boto3.client("s3", region_name=AWS_REGION)

        # windows[window_start][symbol] = [messages]
        self.windows = defaultdict(lambda: defaultdict(list))
        self.running = True

        # periodic flush thread
        self.flush_thread = threading.Thread(target=self._periodic_flush, daemon=True)
        self.flush_thread.start()

        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    # ---------------- WINDOW HELPERS ----------------
    def _get_window_start(self, ts: datetime) -> datetime:
        floored_minute = ts.minute - (ts.minute % WINDOW_MINUTES)
        return ts.replace(minute=floored_minute, second=0, microsecond=0)

    def _upload_to_s3(self, messages, window_start, symbol):
        if not messages:
            return
        key = f"{S3_PREFIX}{symbol}/{window_start.strftime('%Y%m%dT%H%M')}.jsonl"
        buffer = io.StringIO()
        for msg in messages:
            buffer.write(json.dumps(msg) + "\n")
        buffer.seek(0)
        try:
            self.s3.upload_fileobj(buffer, S3_BUCKET, key)
            logger.info(
                f"✅ Uploaded {len(messages)} {symbol} msgs for window {window_start} "
                f"to s3://{S3_BUCKET}/{key}"
            )
        except (BotoCoreError, ClientError) as e:
            logger.error(f"❌ Failed upload {symbol} window {window_start}: {e}", exc_info=True)
        finally:
            buffer.close()

    def _flush_expired_windows(self, force=False):
        """
        Flush windows that are expired OR all windows if force=True
        """
        now = datetime.now(timezone.utc)
        expired = []
        for win_start, symbol_batches in self.windows.items():
            window_end = win_start + timedelta(minutes=WINDOW_MINUTES)
            if force or now > window_end + timedelta(seconds=FLUSH_DELAY):
                for symbol, msgs in symbol_batches.items():
                    self._upload_to_s3(msgs, win_start, symbol)
                expired.append(win_start)

        for win in expired:
            del self.windows[win]

    def _periodic_flush(self):
        while self.running:
            time.sleep(FORCE_FLUSH_INTERVAL)
            logger.info("⏰ Periodic flush triggered")
            self._flush_expired_windows(force=True)

    def _shutdown(self, signum, frame):
        logger.info("🛑 Shutdown received, flushing all windows...")
        self.running = False
        self._flush_expired_windows(force=True)
        self.consumer.close()
        sys.exit(0)

    # ---------------- MAIN LOOP ----------------
    def run(self):
        logger.info("🚀 Kafka consumer started (event-time + periodic flush)")
        try:
            for message in self.consumer:
                data = message.value
                ts = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
                ts = ts.astimezone(timezone.utc)

                symbol = data.get("symbol")
                if not symbol:
                    continue

                window_start = self._get_window_start(ts)
                self.windows[window_start][symbol].append(data)

                # flush expired windows (event-time driven)
                self._flush_expired_windows(force=False)

        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}", exc_info=True)
        finally:
            self._flush_expired_windows(force=True)
            self.consumer.close()


if __name__ == "__main__":
    consumer = KafkaS3HybridConsumer()
    consumer.run()
