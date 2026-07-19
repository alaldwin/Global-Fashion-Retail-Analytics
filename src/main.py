from pyspark.sql import SparkSession

from src.ingestion.extract import read_csv
from src.validation.validation import validation_columns, validation_schema

from src.common.logger import get_logger

logger = get_logger(__name__)

def main():

    logger.info("Pipeline started.")

    spark = (
        SparkSession.builder
        .appName("Retail")
        .getOrCreate()
    )


    try:

        logger.info("Reading CSV files...")
        tables = read_csv(spark)

        logger.info("Starting validation...")


        # Validate
        for tablename, df in tables.items():
            print(f"Validation {tablename}")
            validation_columns(tablename, df)
            validation_schema(tablename, df)

        # Transform
        # transform_data(tables)

        # Load
        # write_parquet(tables)

    finally:
        print("Stopping Spark")
        spark.stop()

if __name__ == "__main__":
    main()
