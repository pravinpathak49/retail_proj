from pyspark.sql import SparkSession


def get_spark_session(app_name="MyApp", master="local[*]", config_options=None):
    """
    Returns a SparkSession object with the specified configurations.
    :return: SparkSession
    """
    spark_builder = SparkSession.builder.appName(app_name).master(master)

    # Apply additional configurations if provided
    config_options = config_options if config_options else {}
    for key, value in config_options.items():
        spark_builder = spark_builder.config(key, value)

    # Build the SparkSession
    spark = spark_builder.getOrCreate()
    return spark


def stop_spark_session(spark_session):
    """
    Stops the SparkSession if it is running.
    """
    if spark_session:
        spark_session.stop()
        spark_session = None
        print("Spark session stopped.")
