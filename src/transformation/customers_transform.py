import re

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import * 


class CustomerTransformer:

    def __init__(self, df: DataFrame):
        self.df = df


    def rename_columns(self) -> DataFrame:

        for old in self.df.columns:
            # convert to lowercase
            new = old.strip().lower()
            # Replace spaces and hyphens with underscores
            new = re.sub(r"[\s\-]+", "_", new)
            # Remove special characters
            new = re.sub(r"[^0-9a-z_]+", "", new)
            # Remove duplicate underscores
            new = re.sub(r"_+", "_", new).strip("_")
            # append cleaned column name to the list

            if old != new:
                self.df = self.df.withColumnRenamed(old, new)

            self.df.printSchema()

        return self.df


    

    def clean_trim(self):

        tokens = ["", "na", "n/a", "none", "null", "-", "_", "unknown"]

        for c, t in self.df.dtypes:

            if t == 'string':
                self.df = self.df.withColumn(c, F.regexp_replace(F.col(c), "\xa0", " "))

                self.df = self.df.withColumn(c, F.trim(F.col(c)))

                self.df = self.df.withColumn(c, F.regexp_replace(F.col(c), r"\s+", " "))

                self.df = self.df.withColumn(c, F.when(F.lower(F.col(c)).isin(tokens), None)
                                             .otherwise(F.col(c)))

        self.df.show(10, truncate=False)




    def clean_name(self):
            
        self.df = (
            self.df
            .withColumn(
                "name",
                F.initcap(
                    F.trim(
                        F.regexp_replace(
                            F.regexp_replace(
                                F.col("name"),
                                r"[^A-Za-z\s'-]",
                                ""
                            ),
                            r"\s+",
                            " "
                        )
                    )
                )
            )
        )

        return self.df



    

    def clean_email(self):

        self.df = (
            self.df
            .withColumn(
                "email",
                F.lower(
                    F.trim(F.col("email"))
                )
            )
            # Remove spaces inside the email
            .withColumn(
                "email",
                F.regexp_replace(F.col("email"), r"\s+", "")
            )
            # Remove "fake_" after the @ symbol
            .withColumn(
                "email",
                F.regexp_replace(
                    F.col("email"),
                    r"@fake_",
                    "@"
                )
            )
        )

        return self.df



    def cast_columns(self) -> DataFrame:

        schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("date_of_birth", DateType(), True),
            StructField("job_title", StringType(), True),
        ])




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

        return self.df


    def customer_transform(self) -> DataFrame:

        self.rename_columns()
        self.clean_trim()
        self.clean_name()
        self.cast_columns()
        self.add_metadata()