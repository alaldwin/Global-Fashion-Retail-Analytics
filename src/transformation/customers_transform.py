import re

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import * 

from config import mapping_city

class CustomerTransformer:

    def __init__(self, df: DataFrame):
        self.df = df


    def rename_columns(self):

        for old in self.df.columns:

            new = old.strip().lower()
            new = re.sub(r"[\s\-]+", "_", new)
            new = re.sub(r"[^0-9a-z_]+", "", new)
            new = re.sub(r"_+", "_", new).strip("_")

            if old != new:
                self.df = self.df.withColumnRenamed(old, new)

        return self


    

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

        return self




    def clean_name(self):

        self.df = (
            self.df.withColumn("name", F.initcap(F.trim(F.regexp_replace(F.regexp_replace(F.col("name"), r"[^A-Za-z\s'-]", ""), r"\s+", " "))))
            )

        return self



    

    def clean_email(self):

        self.df = (
            self.df
            .withColumn("email", F.lower(F.trim(F.col("email"))))
            # Remove spaces inside the email
            .withColumn("email", F.regexp_replace(F.col("email"), r"\s+", ""))
            # Remove "fake_" after the @ symbol
            .withColumn("email", F.regexp_replace(F.col("email"), r"@fake_", "@"))
        )

        return self




    def clean_phone(self):

        self.df = (
            self.df
            .withColumn("telephone", F.trim(F.col("telephone")))
                        
            .withColumn("telephone", F.regexp_replace(F.col("telephone"), r"[()]", " "))

            .withColumn("telephone", F.regexp_replace(F.col("telephone"), r"\s+", "+"))

            .withColumn("telephone", F.when(F.col("telephone") == "", None).otherwise(F.col("telephone")))
        )
        return self



    def clean_city(self):

        # Standardize capitalization
        self.df = self.df.withColumn(
            "city",
            F.initcap(F.lower(F.col("city")))
        )

        city_mapping = mapping_city()

        mapping_expr = F.create_map(
            *[F.lit(x) for kv in city_mapping.items() for x in kv]
        )

        self.df = self.df.withColumn("city", F.coalesce(mapping_expr[F.col("city")], F.col("city")))

        return self



    def clean_country(self):

        country_mapping = {
            "中国": "China",
            "España": "Spain",
            "Deutschland": "Germany",
        }

        country_expr = F.create_map( 
            *[F.lit(x) for kv in country_mapping.items() for x in kv]
        )

        self.df= (
            self.df
            .withColumn(F.coalesce(country_expr[F.col("country")], F.col("country")))
            
        )

        return self




    def clean_gender(self):

        gender = {
            "Male": "M",
            "Female": "F",
            "male": "M",
            "female": "F",
            "m": "M",
            "f": "F",
        }


        expr = F.create_map(
            *[F.lit(x) for kv in gender.items() for x in kv]
        )

        self.df = self.df.withColumn(
            "gender", F.coalesce(expr[F.col("gender")], F.col("gender"))
        )

        return self

    


    def clean_job_title(self):

        self.df = (
            self.df
            .withColumn(
                "job_title",
                F.trim(F.col("job_title"))
            )
            .withColumn(
                "job_title",
                F.regexp_replace(F.col("job_title"), r"\s+", " ")
            )
        )

        return self



    def cast_columns(self):

        self.df = (
            self.df
            .withColumn("customer_id", F.col("customer_id").cast("int"))
            .withColumn("date_of_birth", F.to_date("date_of_birth"))
        )

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



    def customer_transform(
        self,
        batch_date,
        source_system,
        load_type
    ):

        return (
            self
            .rename_columns()
            .clean_trim()
            .clean_name()
            .clean_email()
            .clean_phone()
            .clean_city()
            .clean_country()
            .clean_gender()
            .clean_job_title()
            .cast_columns()
            .add_metadata(
                batch_date,
                source_system,
                load_type
            )
            .df
        )