from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


from src.ingestion.extract import read_csv
from src.validation.validation import DataValidator
from src.transformation.transform_manager import transform_tables


from src.common.logger import get_logger

logger = get_logger(__name__, "pipeline.log")


def main():

    logger.info("Pipeline started.")

    spark = (
        SparkSession.builder
        .appName("Retail")
        .getOrCreate()
    )


    try:

        logger.info("Reading CSV files...")

        print("\n READING TABLES: ")
        tables = read_csv(spark)

        
        transformed_tables = {}

        for tablename, df in tables.items():

            print("=" * 40)
            print(f"Validation {tablename}")
            print("=" * 40)

            if tablename == "transactions":
                df = df.withColumn(
                    "Date",
                    F.to_date(F.col("Date"))
                )

        # Validate
            print("\n VALIDATION: ")
            logger.info("Starting Validation...")
            DataValidator(tablename, df).run_validation()



        for tablename, df in tables.items():

        # Transform
            print("\n TRANSFORMATIONS:  ")
            logger.info("Starting Transformation...")
            transformed_tables[tablename] = transform_tables(
                    table_name=tablename,
                    df=df,
                    batch_date="2026-07-31",
                    source_system="csv",
                    load_type="full",
                )

        # Load
        # write_parquet(tables)

    finally:
        print("Stopping Spark")
        spark.stop()

if __name__ == "__main__":
    main()
