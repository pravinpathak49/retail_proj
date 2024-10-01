from pyspark.sql import DataFrame
from etl.spark_utils import get_spark_session

class Ingestion:
    def __init__(self):
        self.spark = get_spark_session()

    def read_file(self, file_path: str, file_format: str = "csv", delimiter: str = ",", header: bool = True) -> DataFrame:
        """
        Reads a flat file and returns a DataFrame.
        """
        return self.spark.read.format(file_format) \
                              .option("header", header) \
                              .option("delimiter", delimiter) \
                              .load(file_path)
