from src.common.logger import get_logger

from src.validation.schema import expected_columns, expected_schema

logger = get_logger(__name__)

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



def validation_schema(tablename: str, df):

    logger.info("Start Validate Schemas.")

    expected = expected_schema.get(tablename)

    if expected is None:
        raise ValueError(f"No schema defined for '{tablename}'")

    actual = {
        field.name: field.dataType
        for field in df.schema.fields
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
            f"[{tablename}] Schema Validation Failed: \n"
            + "\n".join(errors)
        )

    logger.info(f"[{tablename}] Schema Validation Passed.")