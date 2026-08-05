
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

spark = (
    SparkSession.builder
    .appName("Retail")

    .config("spark.hadoop.fs.s3a.access.key",
            os.getenv("AWS_ACCESS_KEY_ID"))

    .config("spark.hadoop.fs.s3a.secret.key",
            os.getenv("AWS_SECRET_ACCESS_KEY"))

    .config("spark.hadoop.fs.s3a.endpoint",
            "s3.amazonaws.com")

    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    .getOrCreate()
)