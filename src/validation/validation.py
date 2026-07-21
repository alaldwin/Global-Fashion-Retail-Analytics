from pyspark.sql import functions as F


from src.common.logger import get_logger
from src.validation.schema import (
    expected_columns, 
    expected_schema, 
    required_columns, 
    unique_columns
)

logger = get_logger(__name__)

class DataValidator:


    def __init__(self, tablename: str, df):
        self.tablename = tablename
        self.df = df

        

    def validation_columns(tablename: str, df):

        logger.info("Start Validate Columns.")

        excepted = expected_columns.get(tablename)

        if excepted is None:
            raise ValueError(f"No Schema defined for {tablename}")

        actual = set(df.columns)

        missing = excepted - actual
        extra = actual - excepted

        if missing:
            raise ValueError(f"[{tablename}] Missing Columns: {missing}")

        if extra:
            raise ValueError(f"[{tablename}] extra Columns: {extra}")

        logger.info(f"[{tablename}] Column validation passed.")



    def validation_schema(self):

        logger.info("Start Validate Schemas.")

        expected = expected_schema.get(self.tablename)

        if expected is None:
            raise ValueError(f"No schema defined for '{self.tablename}'")

        actual = {
            field.name: field.dataType
            for field in self.df.schema.fields
        }

        errors = []

        for columns, expected_type in expected.items():

            if columns not in actual:
                errors.append(f"{columns}: column is missing")
                continue

            actual_type = actual[columns]

            if actual_type != expected_type:
                errors.append(
                    f"{columns}: expected {expected_type.simpleString()}, "
                    f"got {actual_type.simpleString()}"
                )

        if errors:
            raise TypeError(
                f"[{self.tablename}] Schema Validation Failed: \n"
                + "\n".join(errors)
            )

        logger.info(
            "[%s] Column validation passed.",
            self.table_name
        )



    def validation_nulls(self):

        logger.info("Start Validation Nulls. ")

        required = required_columns.get(self.tablename)

        if required is None:
            raise ValueError(f"No required col defined for '{self.tablename}'")

        result = (
            self.df.select([
                F.count(F.when(F.col(col).isNull(), col)).alias(col)
                for col in required
            ])
            .first()
            .asDict()
        )

        errors = [
            f"{col}: {count} null value(s)"
            for col, count in result.items()
            if count > 0
        ]

        if errors:
            raise ValueError(
                f"[{self.tablename}] Null validation failed:\n"
                + "\n".join(errors)
            )

        logger.info(f"[{self.tablename}] Null validation passed.")



    def validation_dup(self):

        logger.info("Start validation Duplicates.")

        unique = unique_columns.get(self.tablename)

        if unique is None:
            raise ValueError(f"No unique columns defined for '{self.tablename}'")

        duplicates = (
            self.df.groupBy(*unique)
            .count()
            .filter(F.col("count") > 1)
        )

        duplicate_count = duplicates.count()

        if duplicate_count > 0:
            logger.error(f"[{self.tablename}] Found {duplicate_count} duplicate key(s).")

            # Show duplicate keys
            if duplicate_count > 0:
                duplicates.show(20, truncate=False)
                # Stop here temporarily
                return

            # Show full duplicate records
            (
                self.df.join(duplicates.select(*unique), on=unique, how="inner")
                .orderBy(*unique)
                .show(20, truncate=False)
            )

            raise ValueError(
                f"[{self.tablename}] Duplicate validation failed. "
                f"Found {duplicate_count} duplicate key(s)."
            )

        logger.info(f"[{self.tablename}] Duplicate validation passed.")



    def run_validation(self):
            
        self.validate_columns()
        self.validate_schema()
        self.validate_nulls()
        self.validate_duplicates()