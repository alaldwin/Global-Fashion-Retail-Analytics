
from src.transformation.customers_transform import CustomerTransformer

TRANSFORMATION = {
    "customer": CustomerTransformer,
}

def transform_tables(tables, batch_date, source_system, load_type):

    transformed = {}

    for table_name, df in tables.items():
        transformer_cols = TRANSFORMATION.get(table_name)

        if transformer_cols:
            transformer = transformer_cols(df)
            transform_method = getattr(transformer, f"{table_name[:-1]}_transform")
            transformed[table_name] = transform_method(
                batch_date=batch_date,
                source_system=source_system,
                load_type=load_type,
            )
        else:
            transformed[table_name] = df

    return transformed
