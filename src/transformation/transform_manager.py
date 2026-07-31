
from src.transformation.customers_transform import CustomerTransformer
from src.transformation.discounts_transform import DiscountTransformer
from src.transformation.employees_transform import EmployeesTransformer
from src.transformation.products_transform import ProductsTransformer
from src.transformation.stores_transform import StoresTransformer
from src.transformation.transactions_transform import TransactionsTransformer

from src.common.logger import get_logger

logger = get_logger(__name__, "transformation.log")

TRANSFORMATION = {
    "customers": CustomerTransformer,
    "discounts": DiscountTransformer,
    "employees": EmployeesTransformer,
    "products": ProductsTransformer,
    "stores": StoresTransformer,
    "transactions": TransactionsTransformer,
}


logger.info("\n Transformation SCANNING...")


def transform_tables(table_name, df, batch_date, source_system, load_type):

    transformer_cls = TRANSFORMATION.get(table_name)

    if transformer_cls is None:
        return df

    transformer = transformer_cls(df)

    transform_method = getattr(
        transformer,
        f"{table_name[:-1]}_transform"
    )

    return transform_method(
        batch_date=batch_date,
        source_system=source_system,
        load_type=load_type,
    )