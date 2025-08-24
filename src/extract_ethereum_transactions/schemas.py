from faust import Record
from typing import Optional, List

class EthereumTransaction(Record, serializer='json'):
    """
    Represents a single Ethereum transaction record fetched from BigQuery.
    """
    hash: str
    nonce: int
    transaction_index: int
    from_address: str
    to_address: Optional[str]
    value: float   # stored as ETH (converted from Wei)
    gas: Optional[int]
    gas_price: Optional[int]
    input: str
    receipt_cumulative_gas_used: Optional[int]
    receipt_gas_used: Optional[int]
    receipt_contract_address: Optional[str]
    receipt_root: Optional[str]
    receipt_status: Optional[int]
    block_timestamp: str   # ISO-8601 formatted string
    block_number: int
    block_hash: str
    max_fee_per_gas: Optional[int]
    max_priority_fee_per_gas: Optional[int]
    transaction_type: Optional[int]
    receipt_effective_gas_price: Optional[int]
    max_fee_per_blob_gas: Optional[int]
    blob_versioned_hashes: Optional[List[str]]
    receipt_blob_gas_price: Optional[int]
    receipt_blob_gas_used: Optional[int]



"""
CREATE TABLE `bigquery-public-data.crypto_ethereum.transactions`
(
  `hash` STRING NOT NULL OPTIONS(description="Hash of the transaction"),
  nonce INT64 NOT NULL OPTIONS(description="The number of transactions made by the sender prior to this one"),
  transaction_index INT64 NOT NULL OPTIONS(description="Integer of the transactions index position in the block"),
  from_address STRING NOT NULL OPTIONS(description="Address of the sender"),
  to_address STRING OPTIONS(description="Address of the receiver. null when its a contract creation transaction"),
  value NUMERIC OPTIONS(description="Value transferred in Wei"),
  gas INT64 OPTIONS(description="Gas provided by the sender"),
  gas_price INT64 OPTIONS(description="Gas price provided by the sender in Wei"),
  input STRING OPTIONS(description="The data sent along with the transaction"),
  receipt_cumulative_gas_used INT64 OPTIONS(description="The total amount of gas used when this transaction was executed in the block"),
  receipt_gas_used INT64 OPTIONS(description="The amount of gas used by this specific transaction alone"),
  receipt_contract_address STRING OPTIONS(description="The contract address created, if the transaction was a contract creation, otherwise null"),
  receipt_root STRING OPTIONS(description="32 bytes of post-transaction stateroot (pre Byzantium)"),
  receipt_status INT64 OPTIONS(description="Either 1 (success) or 0 (failure) (post Byzantium)"),
  block_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the block where this transaction was in"),
  block_number INT64 NOT NULL OPTIONS(description="Block number where this transaction was in"),
  block_hash STRING NOT NULL OPTIONS(description="Hash of the block where this transaction was in"),
  max_fee_per_gas INT64 OPTIONS(description="Total fee that covers both base and priority fees"),
  max_priority_fee_per_gas INT64 OPTIONS(description="Fee given to miners to incentivize them to include the transaction"),
  transaction_type INT64 OPTIONS(description="Transaction type"),
  receipt_effective_gas_price INT64 OPTIONS(description="The actual value per gas deducted from the senders account. Replacement of gas_price after EIP-1559"),
  max_fee_per_blob_gas INT64 OPTIONS(description="The maximum fee a user is willing to pay per blob gas"),
  blob_versioned_hashes ARRAY<STRING> OPTIONS(description="A list of hashed outputs from kzg_to_versioned_hash"),
  receipt_blob_gas_price INT64 OPTIONS(description="Blob gas price"),
  receipt_blob_gas_used INT64 OPTIONS(description="Blob gas used")
)
PARTITION BY DATE(block_timestamp)
OPTIONS(
  description="Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes.\nThis table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.\nData is exported using https://github.com/medvedev1088/ethereum-etl\n"
);
"""