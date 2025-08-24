import sys
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from schemas import ethereum_transaction_schema
import clickhouse_connect
from urllib.parse import urlparse
from pyspark.sql.types import StringType, TimestampType, LongType, IntegerType, FloatType, DoubleType

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR")
checkpoint_path = os.path.join(CHECKPOINT_DIR, "export_hot_wallets")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL")

def get_clickhouse_client():
    parsed_url = urlparse(CLICKHOUSE_URL)
    return clickhouse_connect.get_client(
        host=parsed_url.hostname,
        port=parsed_url.port,
        database=parsed_url.path.strip('/'),
        user=parsed_url.username,
        password=parsed_url.password,
    )
    
def create_clickhouse_table():
    client = get_clickhouse_client()
    client.command("""
                   CREATE TABLE IF NOT EXISTS labels_wide_table (
                        address String,
                        window_start_time DateTime,
                        window_end_time DateTime,
                        hot_wallet_txn_count_10m UInt64,
                        inserted_at DateTime DEFAULT now()
                   ) 
                   ENGINE = MergeTree() 
                   ORDER BY (address, window_start_time)
                   PARTITION BY toYYYYMM(window_start_time)
                   """)
    client.close()

# Ensure the table exists before starting the stream    
create_clickhouse_table()
print("ClickHouse table created.")

def write_to_clickhouse(batch_df, batch_id):
    """
    Writes a batch of Spark DataFrame to ClickHouse, handling schema evolution.
    """
    print(f"--- Processing batch {batch_id} ---")
    if batch_df.isEmpty():
        print("Empty batch, skipping.")
        return

    # Use persist to avoid recomputing the batch DataFrame
    batch_df.persist()

    try:
        with get_clickhouse_client() as client:
            # 1. Check for schema differences and alter table if necessary
            result = client.query("DESCRIBE labels_wide_table")
            ch_columns = {row[0] for row in result.result_set}

            spark_to_ch_types = {
                StringType: 'String',
                TimestampType: 'DateTime',
                LongType: 'UInt64',
                IntegerType: 'Int32',
                FloatType: 'Float32',
                DoubleType: 'Float64',
            }

            for field in batch_df.schema.fields:
                if field.name not in ch_columns:
                    ch_type = spark_to_ch_types.get(type(field.dataType), 'String')
                    print(f"Adding missing column '{field.name}' with type '{ch_type}' to ClickHouse table.")
                    client.command(f'ALTER TABLE labels_wide_table ADD COLUMN "{field.name}" {ch_type}')

            # 2. Insert data into ClickHouse
            data_to_insert = [tuple(row) for row in batch_df.collect()]
            print(f"Inserting {len(data_to_insert)} rows into ClickHouse.")
            client.insert('labels_wide_table', data_to_insert, column_names=batch_df.columns)
            print(f"Successfully inserted {len(data_to_insert)} rows for batch {batch_id}.")

    except Exception as e:
        print(f"An error occurred while writing to ClickHouse: {e}")
        raise
    finally:
        # Unpersist the DataFrame to free up memory
        batch_df.unpersist()


spark = SparkSession.builder \
    .appName("HotWalletsExport") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1G") \
    .config("spark.executor.cores", "1") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .getOrCreate()

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Decode the JSON message from Kafka, and select the nested fields.
transactions_df = df.select(
    F.from_json(F.col("value").cast("string"), ethereum_transaction_schema).alias("tx")
).select(
    F.col("tx.from_address").alias("from_address"),
    F.col("tx.to_address").alias("to_address"),
    F.col("tx.value").cast("float").alias("value"),
    F.to_timestamp(
        F.regexp_replace(F.col("tx.block_timestamp"), r"\.%fZ$", "Z")
    ).alias("block_timestamp")
).filter(F.col("to_address").isNotNull())

# Group data by a 10-minute window on block_timestamp and to_address
hot_wallets_df = transactions_df \
    .withWatermark("block_timestamp", "1 minute") \
    .groupBy(
        F.window(F.col("block_timestamp"), "2 minute"),
        F.col("to_address")
    ).count()

# Select and rename columns to match the desired output
final_df = hot_wallets_df.select(
    F.col("to_address").alias("address"),
    F.col("window.start").alias("window_start_time"),
    F.col("window.end").alias("window_end_time"),
    F.col("count").alias("hot_wallet_txn_count_10m")
)

# Write the output to ClickHouse using foreachBatch
query = final_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", checkpoint_path) \
    .start()
# query = final_df \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("checkpointLocation", checkpoint_path) \
#     .start()


query.awaitTermination()

