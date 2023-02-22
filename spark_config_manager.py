from pyspark.sql import SparkSession


class SparkSessionManager:
    def __init__(self):
        self.spark_session = None

    def get_or_create_spark_session(self, app_name: str) -> SparkSession:
        if not self.spark_session:
            self.spark_session = (
                SparkSession.builder.master("yarn")
                .appName(app_name)
                .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.11.874")
                .config("spark.dynamicAllocation.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.shuffle.spill.compress", "true")
                .config("spark.shuffle.service.enabled", "true")
                .config("spark.io.compression.codec", "snappy")
                .config("spark.kryoserializer.buffer.max", "250m")
                .enableHiveSupport()
                .getOrCreate()
            )

        return self.spark_session

    def stop_spark_session(self) -> None:
        if self.spark_session:
            self.spark_session.stop()
