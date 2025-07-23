import json
import logging
import os
from datetime import datetime
import pandas as pd

from confluent_kafka import Consumer
from minio import Minio
from minio.error import S3Error

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_TOPIC_BATCH = 'stock-market-batch'
KAFKA_GROUP_ID = "stock-market-consumer-group"

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "stock-market-data"
MINIO_ENDPOINT = "localhost:9000"

def create_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def ensure_bucket_exists(minio_client, bucket_name):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created bucket {bucket_name}")
        else:
            logger.info(f"Bucket {bucket_name} already exists")
    except S3Error as e:
        logger.error(f"Error creating bucket {bucket_name}: {e}")
        raise

def main():
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, MINIO_BUCKET)

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_BATCH])

    logger.info(f"Starting consumer for topic {KAFKA_TOPIC_BATCH}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Got message: {data}")

            symbol = data['symbol']
            date = data['date']
            year, month, day = date.split('-')

            df = pd.DataFrame([data])

            # Local daily file
            local_file = f"/tmp/{symbol}_{year}{month}{day}.csv"

            # Append logic
            if os.path.exists(local_file):
                df.to_csv(local_file, mode='a', header=False, index=False)
            else:
                df.to_csv(local_file, index=False)

            # Upload whole file back to MinIO
            object_name = f"raw/historical/year={year}/month={month}/day={day}/{symbol}.csv"
            minio_client.fput_object(MINIO_BUCKET, object_name, local_file)

            logger.info(f"Wrote data for {symbol} to s3://{MINIO_BUCKET}/{object_name}")

            consumer.commit()

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()




if __name__ == "__main__":
    main()

