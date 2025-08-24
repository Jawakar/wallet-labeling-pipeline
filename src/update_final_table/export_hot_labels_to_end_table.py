import logging
import os
import sys
from typing import Optional
import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from utils_db import create_wallet_labels_postgres_table

# Configure logging for production readiness
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


class ClickhouseConnector:
    """Manages connection to ClickHouse."""

    def __init__(self):
        self.clickhouse_host = os.getenv("CLICKHOUSE_HOST")
        self.clickhouse_port = int(os.getenv("CLICKHOUSE_PORT"))
        self.clickhouse_user = os.getenv("CLICKHOUSE_USER")
        self.clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD")
        self.clickhouse_database = os.getenv("CLICKHOUSE_DATABASE")
        self.secure = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
        self.clickhouse_receive_timeout = int(os.getenv("CLICKHOUSE_RECEIVE_TIMEOUT"))
        self.clickhouse_send_timeout = int(os.getenv("CLICKHOUSE_SEND_TIMEOUT"))
        self._engine: Optional[Engine] = None

        if not self.clickhouse_user or not self.clickhouse_password:
            raise ValueError("CLICKHOUSE_USER and CLICKHOUSE_PASSWORD environment variables must be set.")

    @property
    def engine(self) -> Engine:
        """Provides a SQLAlchemy engine for ClickHouse, creating one if it doesn't exist."""
        if not self._engine:
            try:
                uri = (
                    f'clickhouse+native://{self.clickhouse_user}:{self.clickhouse_password}'
                    f'@{self.clickhouse_host}:{self.clickhouse_port}/{self.clickhouse_database}'
                    f'?receive_timeout={self.clickhouse_receive_timeout}'
                    f'&send_timeout={self.clickhouse_send_timeout}&secure={self.secure}'
                )
                self._engine = create_engine(uri)
                with self._engine.connect() as connection:
                    connection.execute(text('SELECT 1'))
                logger.info("Successfully connected to ClickHouse with SQLAlchemy engine.")
            except Exception as e:
                logger.error(f"Failed to create ClickHouse SQLAlchemy engine: {e}")
                raise
        return self._engine

    def close(self):
        """Disposes of the SQLAlchemy engine."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.info("ClickHouse SQLAlchemy engine disposed.")


class PostgresConnector:
    """Manages connection to PostgreSQL."""

    def __init__(self):
        self.postgres_database_url = os.getenv("POSTGRES_DATABASE_URL")
        if not self.postgres_database_url:
            raise ValueError("POSTGRES_DATABASE_URL environment variable must be set.")
        self._connection: Optional[PgConnection] = None

    def get_connection(self) -> PgConnection:
        """Establishes and returns a PostgreSQL connection."""
        if not self._connection or self._connection.closed:
            logger.info("Connecting to PostgreSQL...")
            try:
                self._connection = psycopg2.connect(self.postgres_database_url)
                logger.info("PostgreSQL connection successful.")
            except psycopg2.OperationalError as e:
                logger.error(f"PostgreSQL connection failed: {e}")
                raise
        return self._connection

    def close_connection(self):
        """Closes the PostgreSQL connection if it's open."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            logger.info("PostgreSQL connection closed.")


class HotWalletLabelsToEndTable:
    """ETL job to identify hot wallets from ClickHouse and update them in PostgreSQL."""

    def __init__(self, threshold_to_consider_hot_wallet: int = 50):
        self.clickhouse_connector = ClickhouseConnector()
        self.postgres_connector = PostgresConnector()
        self.threshold_to_consider_hot_wallet = threshold_to_consider_hot_wallet

    def fetch_hot_wallet_labels_from_clickhouse(self) -> pd.DataFrame:
        """Fetches wallets with transaction counts above the threshold from ClickHouse."""
        logger.info("Fetching hot wallet labels from ClickHouse...")
        query = """
        SELECT
            address,
            toStartOfHour(window_start_time) AS hour_window,
            sum(hot_wallet_txn_count_10m) AS hourly_txn_count
        FROM analytics.labels_wide_table
        GROUP BY
            address,
            hour_window
        HAVING
            hourly_txn_count >= %(threshold)s
        ORDER BY
            hour_window,
            address;
        """
        try:
            df = pd.read_sql_query(
                sql=query,
                con=self.clickhouse_connector.engine,
                params={'threshold': self.threshold_to_consider_hot_wallet}
            )
            logger.info(f"Fetched {len(df)} hot wallet addresses from ClickHouse.")
            return df
        except Exception as e:
            logger.error(f"Error fetching data from ClickHouse: {e}", exc_info=True)
            raise

    def update_hot_wallet_labels_in_postgres(self, hot_wallet_labels: pd.DataFrame):
        """Bulk upserts hot wallet labels into PostgreSQL."""
        if hot_wallet_labels.empty:
            logger.info("No new hot wallet labels to update in PostgreSQL.")
            return

        logger.info(f"Updating {len(hot_wallet_labels)} hot wallet labels in PostgreSQL...")
        
        hot_wallet_labels['label'] = 'hot_wallet'
        hot_wallet_labels['category'] = 'behavioral'
        
        update_data = list(hot_wallet_labels[['address', 'label', 'category']].to_records(index=False))

        conn = self.postgres_connector.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(create_wallet_labels_postgres_table)
                
                update_query = """
                INSERT INTO wallet_labels (address, label, category)
                VALUES %s
                ON CONFLICT (address)
                DO UPDATE SET
                    label = EXCLUDED.label,
                    category = EXCLUDED.category,
                    last_updated_at = CURRENT_TIMESTAMP;
                """
                
                execute_values(cursor, update_query, update_data, page_size=1000)
                conn.commit()
                logger.info(f"Successfully upserted {len(update_data)} records into PostgreSQL.")
        except Exception as e:
            logger.error(f"Error updating data in PostgreSQL: {e}", exc_info=True)
            conn.rollback()
            raise

    def run(self):
        """Executes the ETL job."""
        logger.info("Starting hot wallet labels ETL job.")
        try:
            hot_wallet_labels = self.fetch_hot_wallet_labels_from_clickhouse()
            self.update_hot_wallet_labels_in_postgres(hot_wallet_labels)
            logger.info("Hot wallet labels ETL job finished successfully.")
        except Exception as e:
            logger.error(f"ETL job failed: {e}", exc_info=True)
            sys.exit(1)
        finally:
            self.clickhouse_connector.close()
            self.postgres_connector.close_connection()


if __name__ == "__main__":
    try:
        hot_wallet_job = HotWalletLabelsToEndTable()
        hot_wallet_job.run()
    except Exception as e:
        logger.critical(f"Failed to initialize and run the ETL job: {e}", exc_info=True)
        sys.exit(1)
        
        
