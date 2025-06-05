import os

# Base directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Input paths
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "transactions.csv")
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, "data", "processed", "final_output.csv")

# Output paths for reports and logs
ASSUMPTIONS_PATH = os.path.join(BASE_DIR, "output", "assumptions.txt")
AGG_COUNTRY_PATH = os.path.join(BASE_DIR, "output", "agg_by_country.csv")
AGG_CUSTOMER_PATH = os.path.join(BASE_DIR, "output", "agg_by_customer.csv")
AGG_MONTHLY_PATH = os.path.join(BASE_DIR, "output", "agg_monthly.csv")
LOG_PATH = os.path.join(BASE_DIR, "output", "pipeline.log")

# Output paths for DW model tables
DW_FACT_PATH = os.path.join(BASE_DIR, "data", "warehouse", "fact_sales.csv")
DW_DIM_CUSTOMER_PATH = os.path.join(BASE_DIR, "data", "warehouse", "dim_customer.csv")
DW_DIM_PRODUCT_PATH = os.path.join(BASE_DIR, "data", "warehouse", "dim_product.csv")
DW_DIM_DATE_PATH = os.path.join(BASE_DIR, "data", "warehouse", "dim_date.csv")

# Logging configuration
LOGGING_LEVEL = "INFO"
