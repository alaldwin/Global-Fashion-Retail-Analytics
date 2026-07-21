EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
PHONE_REGEX = r"^\+63\s9\d{2}-\d{3}-\d{4}$"


validation_rules = {
    "customers": {
        "allowed_values": {
            "Gender": ["Male", "Female", "Other"]
        },
        "patterns": {
            "Email": EMAIL_REGEX,
            "Telephone": PHONE_REGEX
        }
    },

    "products": {
        "ranges": {
            "Production Cost": (0, None)
        }
    },

    "transactions": {
        "ranges": {
            "Quantity": (1, None),
            "Unit Price": (0, None),
            "Discount": (0, 100),
            "Invoice Total": (0, None)
        },
        "allowed_values": {
            "Transaction Type": ["Sale", "Refund"],
            "Payment Method": ["Cash", "Credit Card", "GCash"]
        }
    },

    "stores": {
        "ranges": {
            "Number of Employees": (1, None)
        }
    }
}