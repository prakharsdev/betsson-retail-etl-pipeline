import pandas as pd

def log_abnormalities(df: pd.DataFrame) -> list:
    """
    Detect and summarize meaningful abnormalities in the invoice data.
    Based on senior-level data profiling decisions.
    
    Args:
        df (pd.DataFrame): Raw DataFrame after initial cleaning.
    
    Returns:
        list: Descriptions of detected issues.
    """
    issues = []

    # Must-handle (to be removed in clean_data)
    if df["Customer ID"].isnull().any():
        issues.append("Rows with missing Customer ID were removed.")
    if df["Price"].isnull().any():
        issues.append("Rows with missing Price were removed.")
    if df["Description"].isnull().any():
        issues.append("Rows with missing Description were removed.")
    if (df["Quantity"] <= 0).any():
        issues.append("Rows with Quantity ≤ 0 were removed.")
    if (df["Price"] <= 0).any():
        issues.append("Rows with Price ≤ 0 were removed.")
    if df.duplicated().any():
        issues.append("Exact duplicate rows were removed.")
    if df["Description"].str.strip().isin(["", "?", "UNKNOWN"]).any():
        issues.append("Rows with suspicious descriptions ('?', 'UNKNOWN', or blank) were removed.")

    # Flag but retain
    if (df["Quantity"] > 1000).any():
        issues.append("Some rows have Quantity > 1000 - flagged as potential outliers.")
    if (df["Price"] > 1000).any():
        issues.append("Some rows have Price > 1000 - flagged as potential outliers.")
    if df.duplicated(subset=["Invoice", "StockCode"]).any():
        issues.append("Duplicate (Invoice, StockCode) combinations found - retained but logged.")
    flagged_stockcodes = ["POST", "CHECK", "ADJUST"]
    if df["StockCode"].astype(str).str.upper().isin(flagged_stockcodes).any():
        issues.append("System StockCodes like POST, ADJUST, CHECK found - retained but excluded from revenue.")
    multi_country_customers = df.groupby("Customer ID")["Country"].nunique()
    if multi_country_customers.gt(1).any():
        issues.append("Some Customer IDs appear in multiple countries - retained as possible B2B cases.")

    return issues
