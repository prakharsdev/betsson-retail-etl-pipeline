import pandas as pd

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw invoice data by removing critical abnormalities.
    Applies rules based on profiling and business assumptions.

    Returns:
        pd.DataFrame: Cleaned and enriched DataFrame with TotalPrice.
    """

    # Remove exact duplicates
    df = df.drop_duplicates()

    # Drop rows with any critical missing values
    df = df.dropna(subset=["Invoice", "Customer ID", "Description", "Quantity", "Price"])

    # Clean and standardize descriptions
    df["Description"] = df["Description"].astype(str).str.strip()

    # Remove rows with suspicious or blank descriptions
    df = df[~df["Description"].isin(["", "?", "UNKNOWN"])]

    # Remove Quantity and Price anomalies (<= 0)
    df = df[(df["Quantity"] > 0) & (df["Price"] > 0)]

    # Convert InvoiceDate to datetime
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

    # Remove rows with failed datetime parsing (should be none, but just in case)
    df = df[df["InvoiceDate"].notnull()]

    # Calculate TotalPrice normally
    df["TotalPrice"] = (df["Quantity"] * df["Price"]).round(2)

    return df


def aggregate_data(df: pd.DataFrame):
    """
    Generate standard reporting aggregations:
    - Revenue by Country
    - Revenue by Customer
    - Revenue by Month

    Returns:
        dict: Dictionary with aggregation DataFrames
    """

    agg_by_country = df.groupby("Country")["TotalPrice"].sum().reset_index()

    agg_by_customer = df.groupby("Customer ID")["TotalPrice"].sum().reset_index()

    agg_monthly = df.set_index("InvoiceDate").resample("MS")["TotalPrice"].sum().reset_index()

    return {
        "agg_by_country": agg_by_country,
        "agg_by_customer": agg_by_customer,
        "agg_monthly": agg_monthly
    }
