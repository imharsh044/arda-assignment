from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    LongType,
    StringType,
    IntegerType,
)

blocks_schema = StructType(
    [
        StructField("number", LongType(), nullable=True),
        StructField("hash", StringType(), nullable=False),  # primary_key=True
        StructField("parent_hash", StringType(), nullable=True),
        StructField("nonce", StringType(), nullable=True),
        StructField("sha3_uncles", StringType(), nullable=True),
        StructField("logs_bloom", StringType(), nullable=True),
        StructField("transactions_root", StringType(), nullable=True),
        StructField("state_root", StringType(), nullable=True),
        StructField("receipts_root", StringType(), nullable=True),
        StructField("miner", StringType(), nullable=True),
        StructField("difficulty", IntegerType(), nullable=True),
        StructField("total_difficulty", IntegerType(), nullable=True),
        StructField("size", LongType(), nullable=True),
        StructField("extra_data", StringType(), nullable=True),
        StructField("gas_limit", LongType(), nullable=True),
        StructField("gas_used", LongType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("transaction_count", LongType(), nullable=True),
        StructField("base_fee_per_gas", LongType(), nullable=True),
    ]
)

transactions_schema = StructType(
    [
        StructField("hash", StringType(), nullable=False),  # primary_key=True
        StructField("nonce", LongType(), nullable=True),
        StructField("block_hash", StringType(), nullable=True),
        StructField("block_number", LongType(), nullable=True),
        StructField("transaction_index", LongType(), nullable=True),
        StructField("from_address", StringType(), nullable=True),
        StructField("to_address", StringType(), nullable=True),
        StructField("value", IntegerType(), nullable=True),
        StructField("gas", LongType(), nullable=True),
        StructField("gas_price", LongType(), nullable=True),
        StructField("input", StringType(), nullable=True),
        StructField("block_timestamp", TimestampType(), nullable=True),
        StructField("max_fee_per_gas", LongType(), nullable=True),
        StructField("max_priority_fee_per_gas", LongType(), nullable=True),
        StructField("transaction_type", LongType(), nullable=True),
    ]
)

logs_schema = StructType(
    [
        StructField("log_index", LongType(), nullable=False),  # primary_key=True
        StructField("transaction_hash", StringType(), nullable=False),  # primary_key=True
        StructField("transaction_index", LongType(), nullable=True),
        StructField("block_hash", StringType(), nullable=True),
        StructField("block_number", LongType(), nullable=True),
        StructField("address", StringType(), nullable=True),
        StructField("data", StringType(), nullable=True),
        StructField("topics", StringType(), nullable=True),
    ]
)

token_transfers_schema = StructType(
    [
        StructField("token_address", StringType(), nullable=True),
        StructField("from_address", StringType(), nullable=True),
        StructField("to_address", StringType(), nullable=True),
        StructField("value", IntegerType(), nullable=True),
        StructField("transaction_hash", StringType(), nullable=False),  # primary_key=True
        StructField("log_index", LongType(), nullable=False),  # primary_key=True
        StructField("block_number", LongType(), nullable=True),
    ]
)
