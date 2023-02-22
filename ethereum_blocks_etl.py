from pyspark.sql.types import StructType

from config import Constants, ApplicationConfig, KafkaConfig
from dataset_schema import blocks_schema, transactions_schema, logs_schema, token_transfers_schema
from spark_config_manager import SparkSessionManager
from spark_utility import SparkUtility

spark_session = SparkSessionManager().get_or_create_spark_session("ethereum_etl")
spark_utility = SparkUtility(spark_session)

"""
Here I have used Parquet as the columnar store to dump the raw data. 
Alternatively, we can use NoSQL DBs such as Cassandra, CosmosDB etc.
"""


class EthereumETL:
    @staticmethod
    def load_data_to_column_store_and_kafka_topic(file_name: str, schema: StructType) -> None:
        s3_raw_data_base_path = ApplicationConfig.ETHEREUM_FOLDER_PATH.format(s3_bucket=Constants.S3_BUCKET)
        s3_column_store_base_path = ApplicationConfig.ETHEREUM_COLUMN_STORE_FOLDER_PATH.format(
            s3_bucket=Constants.S3_BUCKET
        )

        dataframe = spark_utility.read_csv(f"{s3_raw_data_base_path}/{file_name}.{Constants.CSV_FILE_TYPE}", schema)
        spark_utility.write_dataframe_to_parquet(dataframe, f"{s3_column_store_base_path}/{file_name}")
        spark_utility.write_data_to_kafka_topic(dataframe, f"{KafkaConfig.KAFKA_TOPIC_PREFIX}_{file_name}")

    def load_data_into_columnar_store(self):
        self.load_data_to_column_store_and_kafka_topic(Constants.ETHEREUM_BLOCKS_FILE, blocks_schema)
        self.load_data_to_column_store_and_kafka_topic(Constants.ETHEREUM_TXN_FILE, transactions_schema)
        self.load_data_to_column_store_and_kafka_topic(Constants.ETHEREUM_LOGS_FILE, logs_schema)
        self.load_data_to_column_store_and_kafka_topic(Constants.ETHEREUM_TOKENS_FILE, token_transfers_schema)


if __name__ == "__main__":
    EthereumETL().load_data_into_columnar_store()
