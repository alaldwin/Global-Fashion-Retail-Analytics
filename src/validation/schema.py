from pyspark.sql.types import (
    IntegerType,
    StringType,
    DateType,
    TimestampType,
    DoubleType,
    DecimalType,
)


expected_columns = {
    "customers": {
        "Customer ID",
        "Name",
        "Email",
        "Telephone",
        "City",
        "Country",
        "Gender",
        "Date Of Birth",
        "Job Title",
    },

    "products": {
        "Product ID",
        "Category",
        "Sub Category",
        "Description PT",
        "Description DE",
        "Description FR",
        "Description ES",
        "Description EN",
        "Description ZH",
        "Color",
        "Sizes",
        "Production Cost"
    },

    "transactions": {
        "Invoice ID",
        "Line",
        "Customer ID",
        "Product ID",
        "Size",
        "Color",
        "Unit Price",
        "Quantity",
        "Date",
        "Discount",
        "Line Total",
        "Store ID",
        "Employee ID",
        "Currency",
        "Currency Symbol",
        "SKU",
        "Transaction Type",
        "Payment Method",
        "Invoice Total"
    },

    "stores": {
        "Store ID",
        "Country",
        "City",
        "Store Name",
        "Number of Employees",
        "ZIP Code",
        "Latitude",
        "Longitude"
    },

    "employees": {
        "Employee ID",
        "Store ID",
        "Name",
        "Position"
    },

    "discounts": {
        "Start",
        "End",
        "Discont",
        "Description",
        "Category",
        "Sub Category"
    }
}




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
        "Invoice ID": IntegerType(),
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
        "Invoice Total": DoubleType()
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