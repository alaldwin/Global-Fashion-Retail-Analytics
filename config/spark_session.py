from pathlib import Path
from pyspark.sql import SparkSession

jar = Path(__file__).resolve().parents[1] / "jars" / "postgresql-42.7.7.jar"

print(jar)
print(jar.exists())

spark = (
    SparkSession.builder
    .appName("Retail")
    .config("spark.jars", str(jar))
    .getOrCreate()
)