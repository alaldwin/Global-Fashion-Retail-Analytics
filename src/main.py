from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType


from src.ingestion.extract import read_csv
from src.validation.validation import DataValidator
from src.transformation.transform import transform_data

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
            print("=" * 40)
            print(f"Validation {tablename}")
            print("=" * 40)
            if tablename == "transactions":
                df = df.withColumn(
                    "Date",
                    F.to_date(F.col("Date"))
                )

            DataValidator(tablename, df).run()


        # Transform
        transform_data(tables)

        # Load
        # write_parquet(tables)

    finally:
        print("Stopping Spark")
        spark.stop()

if __name__ == "__main__":
    main()
