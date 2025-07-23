

import logging
import sys
import traceback
from datetime import datetime
import boto3
import numpy as np
import pandas as pd
import snowflake.connector
from pandas._libs.tslibs.nattype import NaTType
import io
import pyarrow.parquet as pq


# CONFIGURATION

SNOWFLAKE_ACCOUNT = ""
SNOWFLAKE_USER = ""
SNOWFLAKE_PASSWORD = ""
SNOWFLAKE_DATABASE = ""
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_TABLE = ""

S3_ENDPOINT = 'http://localhost:9000'
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_BUCKET = "stock-market-data"


# LOGGING

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# INIT CLIENTS

def init_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

def init_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

# CREATE_SNOWFLAKE_TABLE

def create_snowflake_table(conn):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        symbol STRING NOT NULL,
        window_start TIMESTAMP NOT NULL,
        window_15m_end TIMESTAMP,
        window_1h_end TIMESTAMP,
        moving_avg_price_15m FLOAT,
        moving_avg_price_1h FLOAT,
        price_volatility_15m FLOAT,
        price_volatility_1h FLOAT,
        total_volume_15m FLOAT,
        total_volume_1h FLOAT,
        last_updated TIMESTAMP,
        PRIMARY KEY (symbol, window_start)
    )
    """
    cursor = conn.cursor()
    try:
        cursor.execute(create_table_query)
        logger.info(" Snowflake main table checked/created with matching column names.")
    except Exception as e:
        logger.error(f" Failed to create main table: {e}")
        raise
    finally:
        cursor.close()


# READ PARQUET FILES FROM S3

def read_processed_parquet(s3_client, execution_date):
    s3_prefix = f"processed/realtime/"
    logger.info(f" Scanning S3 prefix: s3://{S3_BUCKET}/{s3_prefix}")

    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
        if "Contents" not in response:
            logger.info("No processed files found.")
            return None

        dfs = []
        file_count = 0

        for obj in response["Contents"]:
            if not obj["Key"].endswith(".parquet"):
                continue

            file_count += 1
            logger.info(f" Processing file {file_count}: {obj['Key']}")

            symbol = None
            parts = obj['Key'].split('/')
            for part in parts:
                if part.startswith('symbol='):
                    symbol = part.split('=')[1].strip()
                    break

            try:
                response_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=obj["Key"])
                parquet_data = response_obj["Body"].read()
                parquet_buffer = io.BytesIO(parquet_data)
                table = pq.read_table(parquet_buffer)
                df = table.to_pandas()

                if 'symbol' not in df.columns and symbol:
                    df['symbol'] = symbol
                elif 'symbol' in df.columns:
                    df['symbol'] = df['symbol'].astype(str).str.strip()

                dfs.append(df)

            except Exception as e:
                logger.error(f" Error processing file {obj['Key']}: {str(e)}")
                continue

        if not dfs:
            logger.info("No valid Parquet files to process.")
            return None

        df_all = pd.concat(dfs, ignore_index=True)
        df_all["last_updated"] = datetime.now()

        df_all = df_all[df_all['symbol'].notna() & (df_all['symbol'] != '')]
        if df_all.empty:
            logger.warning(" All rows have empty symbols after cleaning")
            return None

        logger.info(f" Loaded {len(df_all)} rows from {file_count} files")
        return df_all

    except Exception as e:
        logger.error(f" Error reading from S3: {str(e)}")
        logger.error(traceback.format_exc())
        return None

# LOAD TO SNOWFLAKE (MERGE)

def incremental_load_to_snowflake(conn, df):
    if df is None or df.empty:
        logger.info("No data to load.")
        return

    required_columns = {
        'symbol', 'window_start', 'window_15m_end', 'window_1h_end',
        'moving_avg_price_15m', 'moving_avg_price_1h',
        'price_volatility_15m', 'price_volatility_1h',
        'total_volume_15m', 'total_volume_1h', 'last_updated'
    }
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        logger.error(f"DataFrame missing required columns: {missing_columns}")
        return

    df = df.copy()
    df['symbol'] = df['symbol'].astype(str).str.strip()
    valid_df = df[df['symbol'].notna() & (df['symbol'] != '')]

    if valid_df.empty:
        logger.warning(" No valid rows to insert after filtering.")
        return

    cursor = conn.cursor()
    try:
        stage_table = "TEMP_STAGE_TABLE"
        create_stage_query = f"""
        CREATE OR REPLACE TEMPORARY TABLE {stage_table} (
            symbol STRING,
            window_start TIMESTAMP,
            window_15m_end TIMESTAMP,
            window_1h_end TIMESTAMP,
            moving_avg_price_15m FLOAT,
            moving_avg_price_1h FLOAT,
            price_volatility_15m FLOAT,
            price_volatility_1h FLOAT,
            total_volume_15m FLOAT,
            total_volume_1h FLOAT,
            last_updated TIMESTAMP
        )
        """
        cursor.execute(create_stage_query)

        records = valid_df.to_records(index=False)
        columns = list(valid_df.columns)
        placeholders = ",".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {stage_table} ({','.join(columns)}) VALUES ({placeholders})"

        record_list = []
        for record in records:
            record_tuple = tuple(
                None if pd.isna(val)
                else val.item() if isinstance(val, (pd.Timestamp, NaTType))
                else float(val) if isinstance(val, (np.floating, np.float64))
                else int(val) if isinstance(val, (np.integer, np.int64))
                else str(val) if isinstance(val, str)
                else val
                for val in record
            )
            record_list.append(record_tuple)

        logger.info(f" Inserting {len(record_list)} rows to stage table...")
        cursor.executemany(insert_query, record_list)

        merge_query = f"""
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
        USING {stage_table} AS source
        ON target.symbol = source.symbol AND target.window_start = source.window_start
        WHEN MATCHED THEN UPDATE SET
            target.window_15m_end = source.window_15m_end,
            target.window_1h_end = source.window_1h_end,
            target.moving_avg_price_15m = source.moving_avg_price_15m,
            target.moving_avg_price_1h = source.moving_avg_price_1h,
            target.price_volatility_15m = source.price_volatility_15m,
            target.price_volatility_1h = source.price_volatility_1h,
            target.total_volume_15m = source.total_volume_15m,
            target.total_volume_1h = source.total_volume_1h,
            target.last_updated = source.last_updated
        WHEN NOT MATCHED THEN INSERT (
            symbol, window_start, window_15m_end, window_1h_end,
            moving_avg_price_15m, moving_avg_price_1h,
            price_volatility_15m, price_volatility_1h,
            total_volume_15m, total_volume_1h, last_updated
        ) VALUES (
            source.symbol, source.window_start, source.window_15m_end, source.window_1h_end,
            source.moving_avg_price_15m, source.moving_avg_price_1h,
            source.price_volatility_15m, source.price_volatility_1h,
            source.total_volume_15m, source.total_volume_1h, source.last_updated
        )
        """
        cursor.execute(merge_query)
        logger.info(f"Successfully merged {len(record_list)} records into Snowflake")

    except Exception as e:
        logger.error(f"Error during Snowflake load: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        cursor.close()


# MAIN 

def main():
    logger.info("Starting Snowflake Incremental Load")

    s3_client = init_s3_client()
    conn = init_snowflake_connection()

    try:
        create_snowflake_table(conn)
        execution_date = datetime.now().strftime("%Y-%m-%d")
        df = read_processed_parquet(s3_client, execution_date)
        if df is not None:
            incremental_load_to_snowflake(conn, df)
        else:
            logger.info("No new data to load.")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        conn.close()
        logger.info(" Snowflake connection closed.")

if __name__ == "__main__":
    main()