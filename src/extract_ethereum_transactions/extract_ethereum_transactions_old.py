import os
import json
import logging
from logging.handlers import RotatingFileHandler
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from schemas import EthereumTransaction


# --- Configuration ---
LOG_FILE = "/app/logs/extractor_service.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5), # 5MB per file, keep 5 old
        logging.StreamHandler()
    ]
)

# Kafka configuration
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9093")
KAFKA_TOPIC = "ethereum.mainnet.transactions"

# BigQuery configuration
# Using a public dataset
BQ_TABLE = "bigquery-public-data.crypto_ethereum.transactions"

def fetch_latest_transactions(client: bigquery.Client, limit: int = 100) -> list[EthereumTransaction]:
    """
    Fetches the most recent transactions from the BigQuery Ethereum dataset.

    Args:
        client: An authenticated BigQuery client instance.
        limit: The maximum number of transactions to fetch.

    Returns:
        A list of EthereumTransaction records.
    """
    logging.info(f"Fetching the latest {limit} transactions from {BQ_TABLE}...")
    
    # This query selects the fields defined in our schema and orders by the
    # block timestamp to get the most recent transactions.
    # We also convert the 'value' from Wei (integer) to ETH (float).
    query = f"""
        SELECT
            `hash`,
            nonce,
            transaction_index,
            from_address,
            to_address,
            SAFE_DIVIDE(value, 1e18) AS value, -- Convert Wei (NUMERIC) to ETH
            gas,
            gas_price,
            input,
            receipt_cumulative_gas_used,
            receipt_gas_used,
            receipt_contract_address,
            receipt_root,
            receipt_status,
            FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', block_timestamp) AS block_timestamp,
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
            DATE(block_timestamp) = CURRENT_DATE()
        ORDER BY
            block_timestamp DESC
        LIMIT {limit};
    """
    
    try:
        query_job = client.query(query)
        rows = query_job.result() # Waits for the query to complete
        
        # Convert each row to an EthereumTransaction record
        transactions = [EthereumTransaction.from_data(dict(row)) for row in rows]
        logging.info(f"Successfully fetched {len(transactions)} transactions.")
        return transactions
    except GoogleCloudError as e:
        logging.error(f"BigQuery error: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during data fetching: {e}")
        raise

def produce_to_kafka(producer: KafkaProducer, transactions: list[EthereumTransaction]):
    """
    Produces a list of transaction records to the Kafka topic.

    Args:
        producer: An initialized KafkaProducer instance.
        transactions: A list of EthereumTransaction objects to send.
    """
    logging.info(f"Producing {len(transactions)} messages to topic '{KAFKA_TOPIC}'...")
    for tx in transactions:
        try:
            # The key is the transaction hash for good partitioning
            key = tx.hash.encode('utf-8')
            
            # The value is the JSON representation of the transaction record
            value = tx.dumps() # Uses the serializer defined in the Record

            producer.send(KAFKA_TOPIC, key=key, value=value)
        except Exception as e:
            logging.error(f"Failed to send message for transaction {tx.hash}: {e}")

    producer.flush() # Block until all async messages are sent
    logging.info("All messages produced and flushed successfully.")


if __name__ == "__main__":
    logging.info("--- Starting BigQuery to Kafka Extractor Service ---")
    
    try:
        # 1. Initialize BigQuery Client (assumes ADC is set up)
        bq_client = bigquery.Client()

        # 2. Fetch data from BigQuery
        latest_transactions = fetch_latest_transactions(bq_client, limit=100)

        if latest_transactions:
            # 3. Initialize Kafka Producer
            logging.info(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
            kafka_producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                # The Faust record dumps to bytes, so no extra encoding is needed
                value_serializer=None 
            )

            # 4. Produce data to Kafka
            produce_to_kafka(kafka_producer, latest_transactions)
            kafka_producer.close()
        else:
            logging.warning("No transactions were fetched. Shutting down.")

    except NoBrokersAvailable:
        logging.error(f"Could not connect to Kafka broker at {KAFKA_BROKER}. "
                      "Please ensure Kafka is running and accessible.")
    except Exception as e:
        logging.error(f"Extractor service failed: {e}")
    
    logging.info("--- Extractor Service Finished ---")
    logging.shutdown()
