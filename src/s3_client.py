import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import logging
import json
import time

"""

Program Flow
Receive batch -> Send to S3 bucket

"""

class S3Uploader:
    def __init__(self, bucket_name:str):
        self.bucket_name = bucket_name
        self.client = boto3.client("s3")
        self.max_retries = 3

        # Setup logger
        self.logger = logging.getLogger("s3-uploader")
        self._setup_logging()
        

    def _setup_logging(self):
        # Only configure if no handlers exist (prevents duplicate config)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

    def upload_batch(self, batch, key):
        """ 

        Function Flow
        receive batch -> send batch to S3 bucket

        Error Handling
        Attempt to send batch data for self.max_retries times
        For each attempt try to send data to S3 bucket
        If successful,
            send logger message then return True
        If exception, 
            check if it's a ClientError, if yes then send logger message then return False
            If it's other exception then send logger message then return False
        After max loop reached,
            send logger message then return False

        """
        for attempt in range(self.max_retries):
            try:
                self.client.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=json.dumps(batch),
                    ContentType='application/json'
                )
                self.logger.info(f"Uploaded to S3: s3://{self.bucket_name}/{key}")
                return True
            
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['403', '404']:
                    self.logger.error(f"Fatal S3 error ({error_code}): {e}")
                    return False  # Don't retry auth/not-found errors
                    
                self.logger.warning(
                    f"Attempt {attempt}/{self.max_retries} failed for {key}: {error_code}"
                )
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)  # Exponential backoff
            
            except Exception as e:
                self.logger.error(f"Unexpected error uploading {key}: {str(e)}")
                return False

        self.logger.error(self.logger.error(f"Max attempt reached ({attempt}/{self.max_retries}) for key: {key}"))


