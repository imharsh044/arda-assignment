from dataclasses import dataclass


@dataclass
class ApplicationConfig:
    ETHEREUM_FOLDER_PATH: str = "s3://{s3_bucket}/ethereum-blocks/raw-data"
    ETHEREUM_COLUMN_STORE_FOLDER_PATH: str = "s3://{s3_bucket}/ethereum-blocks/column-store"

    INFURA_API_URL: str = "https://mainnet.infura.io/v3/{infura_api_key}/"
    INFURA_API_KEY: str = "27e776af494e445287357789f9a55e95"

    MOVING_AVERAGE_WINDOW_SIZE = 5


@dataclass
class KafkaConfig:
    KAFKA_BOOTSTRAP_SERVER: str = "localhost:9092"
    KAFKA_TOPIC_PREFIX: str = "ethereum_etl"
    CHECKPOINT_DIR: str = "s3://{s3_bucket}/checkpoint/"


@dataclass
class Constants:
    S3_BUCKET: str = "harsh-demo-bucket"
    ETHEREUM_BLOCKS_FILE: str = "blocks"
    ETHEREUM_TXN_FILE: str = "transactions"
    ETHEREUM_LOGS_FILE: str = "logs"
    ETHEREUM_TOKENS_FILE: str = "token_transfers"

    CSV_FILE_TYPE: str = "csv"

    MOVING_AVERAGE_TOPIC_NAME: str = "rta_moving_average"
    GAS_VALUE_TOPIC_NAME: str = "rta_gas_value_per_hour"
    CUMMULATIVE_RUNNING_COUNT_TRANSFER_TOPIC_NAME: str = "rta_running_count_transfers_cummulative"
    GROUPED_RUNNING_COUNT_TRANSFER_TOPIC_NAME: str = "rta_running_count_transfers_grouped"
