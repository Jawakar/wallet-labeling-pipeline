from pyspark.sql.types import DoubleType, LongType, DecimalType, StructType, StructField, StringType, IntegerType, BooleanType, DateType, TimestampType, ArrayType

hot_wallets_schema = StructType([
    StructField("wallet_address", StringType(), True),
    StructField("window_start", TimestampType(), True),
    StructField("window_end", TimestampType(), True),
    StructField("hot_wallet_txn_count_10m", LongType(), True),
    StructField("last_updated", TimestampType(), True)
])

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, FloatType, ArrayType
)

ethereum_transaction_schema = StructType([
    StructField("hash", StringType(), False),
    StructField("nonce", LongType(), False),
    StructField("transaction_index", IntegerType(), False),
    StructField("from_address", StringType(), False),
    StructField("to_address", StringType(), True),
    StructField("value", StringType(), False),  # stored as ETH
    StructField("gas", LongType(), True),
    StructField("gas_price", LongType(), True),
    StructField("input", StringType(), False),
    StructField("receipt_cumulative_gas_used", LongType(), True),
    StructField("receipt_gas_used", LongType(), True),
    StructField("receipt_contract_address", StringType(), True),
    StructField("receipt_root", StringType(), True),
    StructField("receipt_status", IntegerType(), True),
    StructField("block_timestamp", StringType(), False),  # could cast to TimestampType if parsing
    StructField("block_number", LongType(), False),
    StructField("block_hash", StringType(), False),
    StructField("max_fee_per_gas", LongType(), True),
    StructField("max_priority_fee_per_gas", LongType(), True),
    StructField("transaction_type", IntegerType(), True),
    StructField("receipt_effective_gas_price", LongType(), True),
    StructField("max_fee_per_blob_gas", LongType(), True),
    StructField("blob_versioned_hashes", ArrayType(StringType()), True),
    StructField("receipt_blob_gas_price", LongType(), True),
    StructField("receipt_blob_gas_used", LongType(), True),
])
