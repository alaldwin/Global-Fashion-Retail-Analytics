
from pyspark.sql.types import (
    IntegerType,
    StringType,
    DateType,
    TimestampType,
    DoubleType,
    DecimalType,
)







expected_schema = {
    "customers": {
        "Customer ID": IntegerType(),
        "Name": StringType(),
        "Email": StringType(),
        "Telephone": StringType(),
        "City": StringType(),
        "Country": StringType(),
        "Gender": StringType(),
        "Date Of Birth": DateType(),
        "Job Title": StringType(),
    },

    "products": {
        "Product ID": IntegerType(),
        "Category": StringType(),
        "Sub Category": StringType(),
        "Description PT": StringType(),
        "Description DE": StringType(),
        "Description FR": StringType(),
        "Description ES": StringType(),
        "Description EN": StringType(),
        "Description ZH": StringType(),
        "Color": StringType(),
        "Sizes": StringType(),
        "Production Cost": DoubleType()
    },

    "transactions": {
        "Invoice ID": StringType(),
        "Line": IntegerType(),
        "Customer ID": IntegerType(),
        "Product ID": IntegerType(),
        "Size": StringType(),
        "Color": StringType(),
        "Unit Price": DoubleType(),
        "Quantity": IntegerType(),
        "Date": DateType(),
        "Discount": DoubleType(),
        "Line Total": DoubleType(),
        "Store ID": IntegerType(),
        "Employee ID": IntegerType(),
        "Currency": StringType(),
        "Currency Symbol": StringType(),
        "SKU": StringType(),
        "Transaction Type": StringType(),
        "Payment Method": StringType(),
        "Invoice Total": DoubleType(),
    },

    "stores": {
        "Store ID": IntegerType(),
        "Country": StringType(),
        "City": StringType(),
        "Store Name": StringType(),
        "Number of Employees": IntegerType(),
        "ZIP Code": StringType(),
        "Latitude": DoubleType(),
        "Longitude": DoubleType()
    },

    "employees": {
        "Employee ID": IntegerType(),
        "Store ID": IntegerType(),
        "Name": StringType(),
        "Position": StringType()
    },

    "discounts": {
        "Start": DateType(),
        "End": DateType(),
        "Discont": DoubleType(),
        "Description": StringType(),
        "Category": StringType(),
        "Sub Category": StringType()
    }

}