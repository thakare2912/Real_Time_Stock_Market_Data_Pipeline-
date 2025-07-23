import os
import sys
import traceback
from datetime import datetime , timedelta 

from pyspark.sql import SparkSession
from pyspark.sql  import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "stock-market-data"
MINIO_ENDPOINT = "http://minio:9000"

def create_spark_session():
    print("Initialzing Spark Session with S3 Configuartion")


    spark = (SparkSession.builder
             .appName("StockMarketBatchProcessor")
             .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
             .getOrCreate()
             )
    
    spark_conf = spark.sparkContext._jsc.hadoopConfiguration()
    spark_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    spark_conf.set("fs.s3a.path.style.access", "true")
    spark_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_conf.set("fs.s3a.connection.ssl.enabled", "false")
    spark_conf.set("fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.sparkContext.setLogLevel("WARN")

    print("Spark session initialized successfully")

    return spark


def read_data_from_minio_bucket(spark, date=None):
    print("Reading data from MinIO bucket")

    if date:
        process_date = datetime.strptime(date, "%Y-%m-%d")
        year = process_date.year
        month = process_date.month
        day = process_date.day
        s3_path = f"s3a://{MINIO_BUCKET}/raw/historical/year={year}/month={month:02d}/day={day:02d}/*.csv"
    else:
        s3_path = f"s3a://{MINIO_BUCKET}/raw/historical/year=*/month=*/day=*/*.csv"

    print(f"Reading data from {s3_path}")

    try:
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(s3_path)
        )
        print("Sample data:")
        df.show(5, truncate=False)
        df.printSchema()
        return df

    except Exception as e:
        print(f"Error reading data from MinIO bucket: {str(e)}")
        return None


def process_stock_data(df):
    print("\n---- Processing Historical Stock Data")

    if df is None or df.count() == 0:
        print("no data to process")
        return None
    
    try:
        print(f"Record count before deduplication: {df.count()}")

        # Drop duplicate rows (symbol + date)
        df = df.dropDuplicates(["symbol", "date"])

        print(f"Record count after deduplication: {df.count()}")

        

        # Define window for daily metrics
        window_day = Window.partitionBy('symbol', 'date')

        # calculate metrices 

        df = df.withColumn("daily_open", F.first("open").over(window_day))
        df = df.withColumn("daily_high", F.max("high").over(window_day))
        df = df.withColumn("daily_low", F.min("low").over(window_day))
        df = df.withColumn("daily_volume", F.sum("volume").over(window_day))
        df = df.withColumn("daily_close", F.last("close").over(window_day))

        # calulate change from open
        df = df.withColumn("daily_change", (F.col("daily_close") - F.col("daily_open"))/F.col("daily_open") * 100)

        print("sample of process data")

        df.select("symbol", "date", "daily_open", "daily_high", "daily_low", "daily_volume", "daily_close", "daily_change").show(5)

        return df
    
    except Exception as e:
        print(f"error processing data : {str(e)}")
        return None


def write_to_minio_bucket(df, date=None):
    print("\n------ Writing processed data to MinIO bucket")

    if df is None:
        print("No data to write")
        return None

    if date is None:
        processed_date = datetime.now().strftime("%Y-%m-%d")
    else:
        processed_date = date

    # Ensure `date` column is present
    if 'date' not in df.columns:
        df = df.withColumn("date", lit(processed_date))

    # Select only the columns you need in order
    output_columns = [
        "symbol",
        "date",
        "daily_open",
        "daily_high",
        "daily_low",
        "daily_volume",
        "daily_close",
        "daily_change"
    ]

    df = df.select(*output_columns)

    output_path = f"s3a://{MINIO_BUCKET}/processed/historical/date={processed_date}"
    print(f"Writing processed data to: {output_path}")

    try:
        df.write \
            .option("header", "true")  .partitionBy("symbol").mode("overwrite").csv(output_path)
        print(f"Data written to S3: {output_path}")
    except Exception as e:
        print(f"Error writing to S3: {str(e)}")
        return None


def main():
     """Main Function to process historical data"""
     print("\n=============================================")
     print("STARTING STOCK MARKET BATCH PROCESSOR")
     print("=============================================\n")

     date =   None

     spark = create_spark_session()

     try:
         df =  read_data_from_minio_bucket(spark, date)

         if df is not None:
             processed_df = process_stock_data(df)

             if processed_df is not None:
                 write_to_minio_bucket(processed_df)
                 print("data writtern minio bucket")
             else:
                 print("error processing data")

         else:
             print("failed to read from minio bucket")

     except Exception as e:
         print(f"Error occurred: {str(e)}")

     finally :
         print("\nStopping Spark Session")
         spark.stop()
         print("\n=============================================")
         print("BATCH PROCESSING COMPLETE")
         print("=============================================\n")


if __name__ == "__main__" :
    main()
         
         

        
              
             
    

