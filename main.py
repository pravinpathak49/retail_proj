import yaml
from etl.ingestion import Ingestion
from etl.preprocessing import Preprocessing
from etl.saving import Saving
from etl.spark_utils import stop_spark_session

# Load configuration
with open('config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

def run_etl():
    # Step 1: Ingest data
    ingestion = Ingestion()
    df = ingestion.read_file(
        file_path=config['input_file_path'],
        file_format=config['file_format'],
        delimiter=config['delimiter'],
        header=config['header']
    )

    # Step 2: Preprocess data
    preprocessing = Preprocessing()
    df_cleaned = preprocessing.clean_orders(df)
    df_cleaned.show()
    # df_transformed = preprocessing.transform_data(df_cleaned)

    # Step 3: Save data
    # saving = Saving()
    # saving.save_data(df_transformed, output_path=config['output_file_path'])

    # Step 4: Stop Spark session
    stop_spark_session(ingestion.spark)

if __name__ == "__main__":
    run_etl()

