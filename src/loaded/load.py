import os

from src.common.logger import get_logger

logger = get_logger(__name__, "pipeline.log")

logger.info("LOADING SCANNING...")


def write_s3(tables, batch_date):

    

    bucket = os.getenv("S3_BUCKET_NAME")
    prefix = os.getenv("S3_BASE_PREFIX")    

    for table_name, df in tables.items():

        output_path = (
            f"s3a://{bucket}/{prefix}/"
            f"{table_name}/batch_date={batch_date}"
        )

        print(output_path)

        (
            df.write
            .mode("overwrite")
            .parquet(output_path)
        )

        print(f"Loaded {table_name}")