from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class Preprocessing:
    def drop_na(self, df: DataFrame) -> DataFrame:
        """
        Performs cleaning operations like removing nulls, duplicates, etc.
        """
        df_cleaned = df.dropna()  # Example of dropping null values
        return df_cleaned
    
    def clean_orders(self, df:DataFrame) -> DataFrame:
        """
        Performs filtering records except COMPLETE and CLOSED
        """
        df_filtered = df.filter((col("order_status") == "CLOSED") | (col("order_status") == "COMPLETE"))
        return df_filtered


    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply any transformation logic.
        """
        df_transformed = df.withColumnRenamed("old_column", "new_column")  # Example transformation
        return df_transformed
