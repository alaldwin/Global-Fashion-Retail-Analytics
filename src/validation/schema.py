from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DataType


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
        "Line",
        "Total",
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

    "discount": {
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
        "Date Of Birth": DataType(),
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
        "Production Cost": StringType()
    },

    "transactions": {
        "Invoice ID": IntegerType(),
        "Line": StringType(),
        "Customer ID": IntegerType(),
        "Product ID": IntegerType(),
        "Size": StringType(),
        "Color": StringType(),
        "Unit Price": StringType(),
        "Quantity": StringType(),
        "Date": DataType(),
        "Discount": StringType(),
        "Line": StringType(),
        "Total": StringType(),
        "Store ID": IntegerType(),
        "Employee ID": IntegerType(),
        "Currency": StringType(),
        "Currency Symbol": StringType(),
        "SKU": StringType(),
        "Transaction Type": StringType(),
        "Payment Method": StringType(),
        "Invoice Total": StringType()
    },

    "stores": {
        "Store ID": IntegerType(),
        "Country": StringType(),
        "City": StringType(),
        "Store Name": StringType(),
        "Number of Employees": StringType(),
        "ZIP Code": StringType(),
        "Latitude": StringType(),
        "Longitude": StringType()
    },

    "employees": {
        "Employee ID": IntegerType(),
        "Store ID": IntegerType(),
        "Name": StringType(),
        "Position": StringType()
    },

    "discount": {
        "Start": DataType(),
        "End": DataType(),
        "Discont": StringType(),
        "Description": StringType(),
        "Category": StringType(),
        "Sub Category": StringType()
    }
}