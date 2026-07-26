from pyspark.sql import SparkSession

from src.common.logger import get_logger

logger = get_logger(__name__)

print("Starting...")

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Test")
    .getOrCreate()
)

print("Spark Version:", spark.version)

spark.range(5).show()

spark.stop()

logger.info(f"the pyspark is already run {spark}")
print("Finished!")