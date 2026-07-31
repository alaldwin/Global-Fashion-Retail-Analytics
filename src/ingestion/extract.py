from pathlib import Path
from src.common.logger import get_logger

from pyspark.sql import SparkSession

logger = get_logger(__name__, "ingestion.log")

logger.info("\n Ingestion SCANNING...")

def read_csv(spark: SparkSession):

    try:

        logger.info("Start Reading file.") 

        project_root = Path(__file__).resolve().parents[2]
        path = project_root / "data" / "raw"

        print("Path:", path)    

        if not path.exists():
            raise FileNotFoundError(f"{path} does not exist.")

        tables = {}

        files = list(path.glob("*.csv"))

        logger.info(f"Found {len(files)} CSV files.")

        for file in files:
            logger.info(file.name)

            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(str(file))
            )

            tables[file.stem] = df

        logger.info(f"Successfully loaded {len(tables)} tables.")

        return tables

    except Exception:
        logger.error("Failed to read CSV files.")
        raise