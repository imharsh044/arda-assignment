from pyspark.sql import functions as func
from pyspark.sql.types import BooleanType
from web3 import Web3

from config import ApplicationConfig, Constants
from spark_config_manager import SparkSessionManager
from spark_utility import SparkUtility

spark_session = SparkSessionManager().get_or_create_spark_session("ethereum_ad_hoc_analytics")
spark_utility = SparkUtility(spark_session)

s3_column_store_base_path = ApplicationConfig.ETHEREUM_COLUMN_STORE_FOLDER_PATH.format(s3_bucket=Constants.S3_BUCKET)


@func.udf(returnType=BooleanType())
def validate_erc20_token(token_address: str) -> bool:
    infura_url = ApplicationConfig.INFURA_API_URL.format(infura_api_key=ApplicationConfig.INFURA_API_KEY)
    web3_client = Web3(Web3.HTTPProvider(infura_url))
    token_code = web3_client.eth.getCode(token_address)
    return False if token_code == "0x" else True


class EthereumAdHocAnalytics:
    @staticmethod
    def count_number_of_erc20_tokens():
        tokens_dataframe = spark_utility.read_parquet(f"{s3_column_store_base_path}/{Constants.ETHEREUM_TOKENS_FILE}/")

        tokens_dataframe = tokens_dataframe.withColumn(
            "is_erc20_token", validate_erc20_token(tokens_dataframe.token_address)
        )

        erc20_token_count_dataframe = tokens_dataframe.select("is_erc20_token").agg(
            func.count(
                func.when(tokens_dataframe.is_erc20_token == func.lit(True), func.lit(1)).otherwise(func.lit(None))
            ).alias("erc20_token_count")
        )

        spark_utility.write_dataframe_to_parquet(
            erc20_token_count_dataframe, f"{s3_column_store_base_path}/erc20_token_counts/"
        )

    @staticmethod
    def calculate_current_balance_for_tokens():
        tokens_dataframe = spark_utility.read_parquet(f"{s3_column_store_base_path}/{Constants.ETHEREUM_TOKENS_FILE}/")

        """
        Here, I have assumed that we need to find the token balance of ERC20 contracts only based on another assumption
        that this is a follow-up question to the finding the valid erc20 tokens.
        
        If this scenario changes, we can remove the clause to filter the erc20 tokens and compute the token value based
        on other token types as well.
        """

        tokens_dataframe = tokens_dataframe.withColumn(
            "is_erc20_token", validate_erc20_token(tokens_dataframe.token_address)
        )

        tokens_dataframe = tokens_dataframe.filter(tokens_dataframe.is_erc20_token == func.lit(True))

        unique_token_address = (
            tokens_dataframe.select(tokens_dataframe.from_address.alias("token_address"))
            .unionAll(tokens_dataframe.select(tokens_dataframe.to_address.alias("token_address")))
            .distinct()
        )

        token_balance_amount = (
            unique_token_address.alias("unique_token_address")
            .join(
                tokens_dataframe.alias("tokens_dataframe"),
                (func.col("unique_token_address.token_address") == func.col("tokens_dataframe.from_address"))
                | (func.col("unique_token_address.token_address") == func.col("tokens_dataframe.to_address")),
                "left",
            )
            .select(
                unique_token_address.token_address,
                func.when(
                    func.col("unique_token_address.token_address") == func.col("tokens_dataframe.from_address"),
                    tokens_dataframe.value * -1,
                )
                .otherwise(tokens_dataframe.value * 1)
                .alias("updated_value"),
            )
        )

        final_token_balance = (
            token_balance_amount.select(token_balance_amount.token_address, token_balance_amount.updated_value)
            .groupBy(token_balance_amount.token_address)
            .agg(func.sum(token_balance_amount.updated_value).alias("final_token_value"))
        )

        spark_utility.write_dataframe_to_parquet(final_token_balance, f"{s3_column_store_base_path}/final_token_value/")

    @staticmethod
    def calculate_highest_transaction_for_block():
        blocks_dataframe = spark_utility.read_parquet(f"{s3_column_store_base_path}/{Constants.ETHEREUM_BLOCKS_FILE}/")
        txns_dataframe = spark_utility.read_parquet(f"{s3_column_store_base_path}/{Constants.ETHEREUM_TXN_FILE}/")

        highest_txn_count_by_block = (
            blocks_dataframe.select("hash", "transaction_count")
            .groupBy("hash")
            .agg(func.max("transaction_count").alias("highest_transaction_count"))
        )

        highest_txn_value_by_block = (
            txns_dataframe.select("block_hash", "value")
            .groupBy("block_hash")
            .agg(func.max("value").alias("highest_transaction_value"))
        )

        highest_txn_details_for_block = (
            highest_txn_count_by_block.alias("highest_txn_count_by_block")
            .join(
                highest_txn_value_by_block.alias("highest_txn_value_by_block"),
                func.col("highest_txn_count_by_block.hash") == func.col("highest_txn_value_by_block.block_hash"),
                "inner",
            )
            .select(
                func.col("highest_txn_value_by_block.block_hash"),
                func.col("highest_txn_count_by_block.highest_transaction_count"),
                func.col("highest_txn_value_by_block.highest_transaction_value"),
            )
        )

        spark_utility.write_dataframe_to_parquet(
            highest_txn_details_for_block, f"{s3_column_store_base_path}/highest_transaction_details_for_block/"
        )


if __name__ == "__main__":
    EthereumAdHocAnalytics().count_number_of_erc20_tokens()
    EthereumAdHocAnalytics().calculate_current_balance_for_tokens()
    EthereumAdHocAnalytics().calculate_highest_transaction_for_block()
