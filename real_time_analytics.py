from pyspark.sql import Window, functions as func

from config import KafkaConfig, Constants, ApplicationConfig
from spark_config_manager import SparkSessionManager
from spark_utility import SparkUtility

spark_session = SparkSessionManager().get_or_create_spark_session("ethereum_real_time_analytics")
spark_utility = SparkUtility(spark_session)


class EthereumRealTimeAnalytics:
    @staticmethod
    def calculate_moving_average_for_blocks():
        blocks_dataframe = spark_utility.read_datastream_from_kafka_topic(
            f"{KafkaConfig.KAFKA_TOPIC_PREFIX}_{Constants.ETHEREUM_BLOCKS_FILE}"
        )

        moving_avg_dataframe = blocks_dataframe.withColumn(
            "moving_avg",
            func.avg("transaction_count").over(
                Window.partitionBy("number").rowsBetween(
                    Window.currentRow, ApplicationConfig.MOVING_AVERAGE_WINDOW_SIZE
                )
            ),
        )

        moving_avg_dataframe = moving_avg_dataframe.select(
            moving_avg_dataframe.number.alias("block_number"), moving_avg_dataframe.moving_avg
        ).distinct()

        spark_utility.write_data_to_kafka_topic(
            moving_avg_dataframe, f"{KafkaConfig.KAFKA_TOPIC_PREFIX}_{Constants.MOVING_AVERAGE_TOPIC_NAME}"
        )

    @staticmethod
    def calculate_gas_for_every_hour():
        transactions_dataframe = spark_utility.read_datastream_from_kafka_topic(
            f"{KafkaConfig.KAFKA_TOPIC_PREFIX}_{Constants.ETHEREUM_TXN_FILE}"
        )

        transactions_dataframe = transactions_dataframe.withColumn(
            "date_hour",
            func.from_unixtime(
                func.unix_timestamp(func.col("block_timestamp"), "yyyy-MM-dd hh:mm:ss"), "yyyy-MM-dd hh:00:00"
            ),
        )

        gas_per_hour_dataframe = (
            transactions_dataframe.select("date_hour", "gas")
            .groupBy("date_hour")
            .agg(func.sum("gas").alias("total_gas_used"))
        )

        spark_utility.write_data_to_kafka_topic(
            gas_per_hour_dataframe, f"{KafkaConfig.KAFKA_TOPIC_PREFIX}_{Constants.GAS_VALUE_TOPIC_NAME}"
        )

    @staticmethod
    def calculate_running_count_of_transfers():
        transaction_dataframe = spark_utility.read_datastream_from_kafka_topic(
            f"{KafkaConfig.KAFKA_TOPIC_PREFIX}_{Constants.ETHEREUM_TXN_FILE}"
        )

        unique_addresses = (
            transaction_dataframe.select(transaction_dataframe.from_address.alias("address"))
            .unionAll(transaction_dataframe.select(transaction_dataframe.to_address.alias("address")))
            .distinct()
        )

        running_count_for_address = (
            unique_addresses.alias("unique_addresses")
            .join(
                transaction_dataframe.alias("transaction_dataframe"),
                (func.col("unique_addresses.address") == func.col("transaction_dataframe.from_address"))
                | (func.col("unique_addresses.address") == func.col("transaction_dataframe.to_address")),
                "left",
            )
            .select(
                unique_addresses.address,
                func.when(
                    func.col("unique_addresses.address") == func.col("transaction_dataframe.from_address"), func.lit(1)
                )
                .otherwise(func.lit(0))
                .alias("message_received"),
                func.when(
                    func.col("unique_addresses.address") == func.col("transaction_dataframe.to_address"), func.lit(1)
                )
                .otherwise(func.lit(0))
                .alias("message_sent"),
                transaction_dataframe.block_timestamp,
            )
        )

        # By "running count", I am assuming it to be a cummulative sum.
        running_cummulative_count_for_txns = running_count_for_address.withColumn(
            "message_received",
            func.sum(running_count_for_address.message_received).over(
                Window.partitionBy(running_count_for_address.address).orderBy(running_count_for_address.block_timestamp)
            ),
        ).withColumn(
            "message_sent",
            func.sum(running_count_for_address.message_sent).over(
                Window.partitionBy(running_count_for_address.address).orderBy(running_count_for_address.block_timestamp)
            ),
        )

        spark_utility.write_data_to_kafka_topic(
            running_cummulative_count_for_txns,
            f"{KafkaConfig.KAFKA_TOPIC_PREFIX}_{Constants.CUMMULATIVE_RUNNING_COUNT_TRANSFER_TOPIC_NAME}",
        )

        # Another approch can be that we sum the count of message received and sent directly using the group by
        running_count_for_address_grouped = (
            running_count_for_address.select(
                running_count_for_address.address,
                running_count_for_address.message_received,
                running_count_for_address.message_sent,
            )
            .groupBy(running_count_for_address.address)
            .agg(
                func.sum(running_count_for_address.message_received).alias("total_messages_received"),
                func.sum(running_count_for_address.message_sent).alias("total_messages_sent"),
            )
        )

        spark_utility.write_data_to_kafka_topic(
            running_count_for_address_grouped,
            f"{KafkaConfig.KAFKA_TOPIC_PREFIX}_{Constants.GROUPED_RUNNING_COUNT_TRANSFER_TOPIC_NAME}",
        )


if __name__ == "__main__":
    EthereumRealTimeAnalytics().calculate_moving_average_for_blocks()
    EthereumRealTimeAnalytics().calculate_gas_for_every_hour()
    EthereumRealTimeAnalytics().calculate_running_count_of_transfers()
