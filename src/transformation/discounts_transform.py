import re

from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_replace
from pyspark.sql import DataFrame
from pyspark.sql.types import *

from src.common.logger import get_logger

logger = get_logger(__name__, "transformation.log")


class DiscoundTransform:

    def __init__(self, df: DataFrame):
        self.df = df


    def rename_columns(self):

        for old in self.df.columns():

            new = old.strip().lower()
            new = re.sub(r"[\s\-]+", "_", new)
            new = re.sub(r"[0-9a-z_]+", "", new)
            new = re.sub(r"_+", "_", new).strip("_")

            if old != new:
                self.df = self.df.withColumnRenamed(old, new)

        return self




    def cast_columns(self):

        self.df = (
            self.df
            .withColumn("discont", "discount")
            .withColumnRenamed("discount", F.col("discount").cast("decimal(10, 2)"))
            .withColumn("start", F.to_date("start"))
            .withColumn("end", F.to_date("end"))
            )
        
        logger.info(f"Schema: {self.df.schema.simpleString()}")

        return self



    def add_metadata(
        self,
        batch_date: str,
        source_system: str,
        load_type: str,
    ):

        self.df = (
            self.df
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("batch_date", F.lit(batch_date).cast("date"))
            .withColumn("source_system", F.lit(source_system))
            .withColumn("load_type", F.lit(load_type))
        )  
        
        return self



    def discount_tranform(
        self,
        batch_date,
        source_system,
        load_type
    ):

        return (
            self
            .rename_columns()
            .add_metadata(
                batch_date,
                source_system,
                load_type
            )
            .df
        )