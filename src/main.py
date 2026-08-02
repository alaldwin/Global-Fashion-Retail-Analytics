from datetime import date

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

from src.ingestion.extract import read_csv
from src.validation.validation import DataValidator
from src.transformation.transform_manager import transform_tables
from src.loaded.load import write_postgresql


from src.common.logger import get_logger

logger = get_logger(__name__, "pipeline.log")


def main():

    logger.info("Pipeline started.")

    Spark= (
        SparkSession.builder
        .appName("Retail Data Pipeline")
        .getOrCreate()
    )

    try:

        print("\n READING TABLES... ")
        tables = read_csv(Spark)

        
        transformed_tables = {}

        for tablename, df in tables.items():

            print("=" * 80)
            print(f"Validation {tablename}")
            print("=" * 80)

            if tablename == "transactions":
                df = df.withColumn(
                    "Date",
                    F.to_date(F.col("Date"))
                )

        # Validate
            print("\n VALIDATION... ")
            DataValidator(tablename, df).run_validation()



        for tablename, df in tables.items():

        # Transform
            print("\n TRANSFORMATIONS... ")

            batch_date = date.today().isoformat()

            transformed_tables[tablename] = transform_tables(
                    table_name=tablename,
                    df=df,
                    batch_date=batch_date,
                    source_system="csv",
                    load_type="full",
                )

        # Load
        print("\n LOADING... ")
        write_postgresql(
            transformed_tables, 
            batch_date=batch_date
            )

    finally:
        print("Stopping Spark")
        Spark.stop()

if __name__ == "__main__":
    main()
