import os
import logging
from logging.handlers import RotatingFileHandler
import faust
import aioredis
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from schemas import EthereumTransaction
from typing import List, Optional

# --- Configuration ---
LOG_FILE = "/app/logs/near_real_time_extractor_service.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)

# Kafka configuration
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")
KAFKA_TOPIC_PARTITIONS = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", 1)) # TODO: on production, set to 32-64
KAFKA_TOPIC_REPLICATION_FACTOR = int(os.environ.get("KAFKA_TOPIC_REPLICATION_FACTOR", 1)) # TODO: on production, set to 3

# BigQuery configuration
BQ_TABLE = "bigquery-public-data.crypto_ethereum.transactions"

# Redis configuration
REDIS_URL = os.environ.get('REDIS_URL')
REDIS_LAST_TIMESTAMP_KEY = "nansen:extract_ethereum_transactions:last_timestamp"
REDIS_PROCESSED_TX_KEY_PREFIX = "nansen:extract_ethereum_transactions:processed_tx"
#REDIS_PROCESSED_TX_TTL_SECONDS = 86400  # 24 hours

# Faust App
app = faust.App(
    'nansen.extract_ethereum_transactions',
    broker=f"kafka://{KAFKA_BROKER}",
)

redis_client = None

def create_kafka_topic_if_not_exists():
    """Creates the Kafka topic if it doesn't exist."""
    try:
        logging.info(f"Checking if Kafka topic '{KAFKA_TOPIC}' exists...")
        admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER])
        consumer = KafkaConsumer(bootstrap_servers=[KAFKA_BROKER], group_id="bigquery-extractor-admin")
        topic_list = consumer.topics()

        if KAFKA_TOPIC in topic_list:
            logging.info(f"Kafka topic '{KAFKA_TOPIC}' already exists.")
            return

        logging.info(f"Kafka topic '{KAFKA_TOPIC}' not found. Attempting to create it...")
        new_topic = NewTopic(
            name=KAFKA_TOPIC,
            num_partitions=KAFKA_TOPIC_PARTITIONS,
            replication_factor=KAFKA_TOPIC_REPLICATION_FACTOR
        )
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        logging.info(f"Successfully created Kafka topic '{KAFKA_TOPIC}' with {KAFKA_TOPIC_PARTITIONS} partitions and replication factor {KAFKA_TOPIC_REPLICATION_FACTOR}.")

    except TopicAlreadyExistsError:
        logging.warning(f"Kafka topic '{KAFKA_TOPIC}' already exists. Race condition handled.")
    except NoBrokersAvailable:
        logging.error(f"Could not connect to Kafka broker at {KAFKA_BROKER}. Topic creation failed.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during Kafka topic creation: {e}")
        raise

@app.task
async def on_start():
    """Initializes the Redis client on application start and ensures Kafka topic exists."""
    global redis_client
    logging.info(f"Connecting to Redis at {REDIS_URL}...")
    redis_client = await aioredis.from_url(REDIS_URL)
    logging.info("Redis client initialized successfully.")
    create_kafka_topic_if_not_exists()

def fetch_transactions(client: bigquery.Client, last_timestamp: Optional[str], limit: int = 1000) -> List[EthereumTransaction]:
    """
    Fetches transactions from BigQuery.
    If last_timestamp is provided, it fetches transactions newer than that timestamp.
    Otherwise, it fetches the most recent transactions for the current day.
    """
    if last_timestamp:
        logging.info(f"Fetching transactions newer than {last_timestamp} from {BQ_TABLE}...")
        where_clause = f"DATE(block_timestamp) = CURRENT_DATE() AND block_timestamp > TIMESTAMP('{last_timestamp}')"
        order_by_clause = "block_timestamp ASC"
    else:
        logging.info(f"Fetching latest transactions for initial run from {BQ_TABLE}...")
        where_clause = "DATE(block_timestamp) = CURRENT_DATE()"
        order_by_clause = "block_timestamp DESC"

    # Note: Using FORMAT_TIMESTAMP with '%Y-%m-%dT%H:%M:%S.%fZ' to preserve microsecond precision
    query = f"""
        SELECT
            `hash`,
            nonce,
            transaction_index,
            from_address,
            to_address,
            SAFE_DIVIDE(value, 1e18) AS value,
            gas,
            gas_price,
            input,
            receipt_cumulative_gas_used,
            receipt_gas_used,
            receipt_contract_address,
            receipt_root,
            receipt_status,
            FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S.%fZ', block_timestamp) AS block_timestamp,
            block_number,
            block_hash,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            transaction_type,
            receipt_effective_gas_price,
            max_fee_per_blob_gas,
            blob_versioned_hashes,
            receipt_blob_gas_price,
            receipt_blob_gas_used
        FROM
            `{BQ_TABLE}`
        WHERE
            {where_clause}
        ORDER BY
            {order_by_clause}
        LIMIT {limit};  # TODO: on production, remove this limit
    """
    try:
        query_job = client.query(query)
        rows = query_job.result()
        transactions = [EthereumTransaction.from_data(dict(row)) for row in rows]
        
        if not last_timestamp and transactions:
            # For the initial run, reverse to process chronologically
            transactions.reverse()
            
        logging.info(f"Successfully fetched {len(transactions)} transactions.")
        return transactions
    except GoogleCloudError as e:
        logging.error(f"BigQuery error: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during data fetching: {e}")
        raise

async def produce_to_kafka(producer: KafkaProducer, transactions: List[EthereumTransaction], redis: aioredis.Redis):
    """
    Produces a list of transaction records to the Kafka topic, checking for duplicates in Redis.
    """
    logging.info(f"Attempting to produce {len(transactions)} messages to topic '{KAFKA_TOPIC}'...")
    produced_count = 0

    for tx in transactions:
        block_tx = f"{tx.block_number}:{tx.hash}"
        try:
            # Check if already processed
            is_member = await redis.sismember(REDIS_PROCESSED_TX_KEY_PREFIX, block_tx)
            if is_member:
                logging.info(f"Skipping already processed transaction {tx.hash} in block {tx.block_number}.")
                continue

            # Produce to Kafka
            key = tx.hash.encode("utf-8")
            value = tx.dumps()
            producer.send(KAFKA_TOPIC, key=key, value=value)

            # Mark as processed
            await redis.sadd(REDIS_PROCESSED_TX_KEY_PREFIX, block_tx)
            produced_count += 1

        except Exception as e:
            logging.error(f"Failed to send message for transaction {tx.hash}: {e}")

    if produced_count > 0:
        producer.flush()
        logging.info(f"{produced_count} messages produced and flushed successfully.")
    else:
        logging.info("No new messages were produced.")



@app.timer(interval=60.0)  # 60 seconds
async def periodic_fetch_and_produce():
    """Periodically fetches new transactions from BigQuery and produces them to Kafka."""
    logging.info("--- Running periodic fetch from BigQuery ---")
    global redis_client
    if not redis_client:
        logging.error("Redis client not initialized. Skipping this run.")
        return

    try:
        # 1. Get the last processed timestamp from Redis
        last_timestamp_bytes = await redis_client.get(REDIS_LAST_TIMESTAMP_KEY)
        last_timestamp = last_timestamp_bytes.decode('utf-8') if last_timestamp_bytes else None
        if last_timestamp and "%f" in last_timestamp:
            last_timestamp = last_timestamp.replace(".%f", "")  # strip broken fractional part

        # 2. Initialize BigQuery Client
        bq_client = bigquery.Client()

        # 3. Fetch data from BigQuery
        transactions = fetch_transactions(bq_client, last_timestamp)

        if transactions:
            # 4. Initialize Kafka Producer and send data
            kafka_producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=None
            )
            await produce_to_kafka(kafka_producer, transactions, redis_client)
            kafka_producer.close()
            
            # 5. Update the last processed timestamp in Redis
            newest_timestamp = transactions[-1].block_timestamp
            await redis_client.set(REDIS_LAST_TIMESTAMP_KEY, newest_timestamp)
            logging.info(f"Updated last processed timestamp to: {newest_timestamp}")
        else:
            logging.info("No new transactions found.")

    except NoBrokersAvailable:
        logging.error(f"Could not connect to Kafka broker at {KAFKA_BROKER}.")
    except Exception as e:
        logging.error(f"Periodic task failed: {e}", exc_info=True)

if __name__ == "__main__":
    logging.info("--- Starting BigQuery to Kafka Extractor Service ---")
    app.main()
