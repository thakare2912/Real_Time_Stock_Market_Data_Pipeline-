import logging
import sys
import traceback
from datetime import datetime , timedelta
import boto3
import numpy as np
import pandas as pd
import snowflake.connector
from pandas._libs.tslibs.nattype import NaTType
import  io

SNOWFLAKE_PASSWORD = "" 

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# S3/MinIO configuration
S3_ENDPOINT = 'http://minio:9000'  # Adjust if MinIO is not accessible at this endpoint
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
S3_BUCKET = 'stock-market-data'

#Snowflake Config
SNOWFLAKE_ACCOUNT = ''
SNOWFLAKE_USER = ''
SNOWFLAKE_DATABASE = ""
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_TABLE = "DAILY_STOCK_METRICS"


def init_s3_client ():
    try :
        s3_client = boto3.client(
            's3',
            endpoint_url = S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY
        )

        logger.info(f"S3 client initialized")
        return s3_client

    except Exception as e:
        logger.error(f"failed to intialize S3 client : {e}")
        raise

def init_snowflake_connection():
    try :
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA ,
            ocsp_fail_open=True,
            insecure_mode=True)
        logger.info(f"Snowflake connect succesfully")
        return conn
    except Exception as e :
        logger.error(f"falied to connect to snowflake:{e}")
        raise

def  create_snowflake_table(conn):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
    symbol STRING,
    date DATE,
    daily_open FLOAT,
    daily_high FLOAT,
    daily_low FLOAT,
    daily_volume FLOAT,
    daily_close FLOAT,
    daily_change FLOAT,
    last_updated TIMESTAMP,
    PRIMARY KEY (symbol, date)
    )
    """

    try :
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Snowflake table create")

    except Exception as e :
        logger.error(f"failed to create table in snowflake")
    
    finally :
        cursor.close()
        

def read_processed_data(s3_client, execution_date):
    logger.info(f"\n ----------------------------------Reading the proces data from minio bucket")

    s3_prefix = f"processed/historical/date={execution_date}"
    logger.info(f'reading data from s3://{S3_BUCKET}/{s3_prefix}')

    #Key → Always provided by S3 listing → it’s the file path.
    #Body → Always provided by get_object → it’s the raw content.

    try :
        response = s3_client.list_objects_v2(Bucket = S3_BUCKET , Prefix = s3_prefix)
        if "Contents" not in response:
            logger.info(f"no data found exeution date :{execution_date}")
            return None
        
        dfs = []

        for obj in response["Contents"]:
            if obj['Key'].endswith(".csv"):
                logger.info(f"reading file: {obj['Key']}")
            if not obj['Key'].endswith(".csv"):
                continue
            try :
                symbol = None
                parts = obj['Key'].split("/")
                for part in parts :
                    if part.startswith("symbol="):
                        symbol = part.split("=")[-1]
                        break
                if not symbol:
                    logger.error(f"Failed to extract symbol from key: {obj['Key']}")
                    continue

                response  = s3_client.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
                csv_data = response['Body'].read()

                csv_buffer = io.BytesIO(csv_data)
                df = pd.read_csv(csv_buffer)
                
                if "symbol" not in df.columns:
                        df['symbol'] = symbol

                dfs.append(df)

            except Exception as e:
                logger.error(f"Fsailed to read file : {obj['key']}:{e}")
                continue

        if not dfs:
            logger.info(f"No data found for execution date {execution_date}")
            return None
    
        processed_df = pd.concat(dfs , ignore_index= True) # pd.concat() is a pandas function that combines multiple DataFrames into one big DataFrame.
        logger.info(f"read {len(processed_df)} row from minio_bucket")

        if "date" in processed_df.columns:
          processed_df["date"] = pd.to_datetime(processed_df["date"]).dt.date
        else :
            logger.error("missing date column in process data ")
            return None
      
        processed_df['last_update'] = datetime.now()
        processed_df =  processed_df.drop_duplicates(subset= ['symbol','date'], keep= 'last')

        required_columns = {
            "symbol": "symbol",
            "date": "date",
            "daily_open": "daily_open",
            "daily_high": "daily_high",
            "daily_low": "daily_low",
            "daily_volume": "daily_volume",
            "daily_close": "daily_close",
            "daily_change":"daily_change"
        }
        processed_df = processed_df[list(required_columns.keys())]

        return processed_df
    
    except Exception as e :
        logger.error(f"Failed to read data from S3: {e}")
        return None

def incremental_load_to_snowflake(conn, df):
    logger.info("\n----------- Performing Incremental Load into Snowflake")

    if df is None or df.empty:
        logger.info("no data load ")
        return None
    
    try :
        #create the new session
        cursor = conn.cursor()

        stage_table = "TEMP_STAGE_TABLE"
        cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} LIKE {SNOWFLAKE_TABLE}")
        
        records = df.to_records(index=False)  # .to_records() turns your DataFrame into a structured NumPy array.
        columns = list(df.columns)
        placeholder = ",".join(['%s'] * len(columns))  
        insert_query = f"INSERT INTO {stage_table} ({','.join(columns)}) VALUES({placeholder})"  #SQL INSERT QUARY
        logger.info(f"Executing INSERT QUERY: {insert_query}")

        record_list = []

        for record in records :
            record_tuple = tuple(
                None if pd.isna(val) else # - If the value is NaN, set it to None (for SQL NULL)
                val.item() if isinstance(val, (pd.Timestamp, NaTType)) else   # If it’s a pandas Timestamp, convert to native Python type
                float(val)  if isinstance (val , (np.floating , np.float64)) else  # - If it’s a NumPy float or int, convert to normal float or int
                int(val) if isinstance(val , (np.integer , np.int64)) else
                val 
                for val in record
            )
            record_list.append(record_tuple)

        logger.info(f"Prepared {len(record_list)} records for insertion")
        logger.info(f"Sample record: {record_list}")

        cursor.executemany(insert_query , record_list)

        merge_query = f"""
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} As target
        USING {stage_table} as source
        ON target.symbol = source.symbol AND target.date = source.date
        WHEN MATCHED THEN
        UPDATE SET 
                target.daily_open = source.daily_open,
                target.daily_high = source.daily_high,
                target.daily_low = source.daily_low,
                target.daily_volume = source.daily_volume,
                target.daily_close = source.daily_close,
                target.daily_change = source.daily_change,
                target.last_updated = source.last_updated

        WHEN NOT MATCHED THEN
             INSERT (symbol,date,daily_open,daily_high,daily_low,daily_volume,daily_close,daily_change, last_updated)
             VALUES (source.symbol,source.date,source.daily_open, source.daily_high, source.daily_low, source.daily_volume, source.daily_close, source.daily_change, source.last_updated)
              
               """
        

        cursor.execute(merge_query)
        logger.info(f"Successfully performed incremental load")

    except Exception as e :
        logger.error (f"failed to create temporary table:{e}")
        return 
    
    finally :
        cursor.close()


def main():
    logger.info("\n=========================================")
    logger.info("STARTING SNOWFLAKE INCREMENTAL LOAD")
    logger.info("=========================================\n")

    execution_date = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d")

    s3_client =  init_s3_client()

    conn = init_snowflake_connection()

    try : 
        create_snowflake_table (conn)

        df = read_processed_data(s3_client , execution_date)

        if df is not None :
            incremental_load_to_snowflake(conn , df )

        else :
            logger.info("\n no data load into sonwflake")

    except Exception as e :
        logger.error(f"Failed to excute commads on Snowflake: {e}")
        sys.exit(1)

    finally:
        conn.close()
        logger.info("snowflake connection closed")

if __name__ == "__main__" :
    main()


