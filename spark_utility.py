from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from config import KafkaConfig, Constants


class SparkUtility:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def read_csv(self, csv_path: str, schema: StructType, header: bool = True, delimiter: str = ",") -> DataFrame:
        return (
            self.spark_session.read.option("header", header).option("delimiter", delimiter).csv(csv_path, schema=schema)
        )

    def read_parquet(self, parquet_path: str) -> DataFrame:
        return self.spark_session.read.parquet(parquet_path)

    @staticmethod
    def write_dataframe_to_parquet(
        dataframe: DataFrame, output_path: str, write_mode: str = "overwrite", num_of_partitions: int = 200
    ) -> None:
        dataframe.repartition(num_of_partitions).write.mode(write_mode).parquet(output_path)

    def read_datastream_from_kafka_topic(self, topic_name: str) -> DataFrame:
        # fmt: off
        return self.spark_session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KafkaConfig.KAFKA_BOOTSTRAP_SERVER) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .load()
        # fmt: on

    @staticmethod
    def write_data_to_kafka_topic(
        dataframe: DataFrame, topic_name: str, processing_time: str = "10 seconds", output_mode: str = "complete"
    ) -> None:
        # fmt: off
        dataframe \
            .writeStream \
            .trigger(processingTime=processing_time) \
            .outputMode(output_mode) \
            .format("kafka") \
            .option("topic", topic_name) \
            .option("kafka.bootstrap.servers", KafkaConfig.KAFKA_BOOTSTRAP_SERVER) \
            .option("checkpointLocation", KafkaConfig.CHECKPOINT_DIR.format(s3_bucket=Constants.S3_BUCKET)) \
            .start() \
            .awaitTermination()
        # fmt: on
