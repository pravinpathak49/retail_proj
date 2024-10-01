from pyspark.sql import DataFrame

class Saving:
    def save_data(self, df: DataFrame, output_path: str, file_format: str = "parquet"):
        """
        Saves the DataFrame to a specified path.
        """
        df.write.format(file_format).mode("overwrite").save(output_path)
