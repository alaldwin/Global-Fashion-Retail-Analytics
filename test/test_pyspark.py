from pyspark.sql import SparkSession

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

print("Finished!")