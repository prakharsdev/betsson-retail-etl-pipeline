import pandas as pd
from datetime import datetime

def profile_raw_data(file_path):
    print("Loading raw data...")
    df = pd.read_csv(file_path, encoding="ISO-8859-1", low_memory=False)
    df.columns = df.columns.str.strip()

    print("\nDataFrame Info:")
    print(df.info())

    print("\nSummary Statistics:")
    print(df.describe(include="all"))

    print("\nNull Value Count:")
    print(df.isnull().sum())

    print("\nDuplicate Rows:")
    print(f"Total exact duplicates: {df.duplicated().sum()}")

    print("\nDuplicate (Invoice, StockCode) Combinations:")
    dupes = df[df.duplicated(subset=["Invoice", "StockCode"], keep=False)]
    print(f"Duplicate (Invoice, StockCode) rows: {dupes.shape[0]}")

    print("\nSuspicious Descriptions:")
    suspicious = df["Description"].str.strip().isin(["", "?", "UNKNOWN"])
    print(f"Suspicious or empty descriptions: {suspicious.sum()}")

    print("\nQuantity and Price Issues:")
    print(f"Quantity <= 0: {df[df['Quantity'] <= 0].shape[0]}")
    print(f"Price <= 0: {df[df['Price'] <= 0].shape[0]}")
    print(f"Quantity > 1000: {df[df['Quantity'] > 1000].shape[0]}")
    print(f"Price > 1000: {df[df['Price'] > 1000].shape[0]}")

    print("\nInvoiceDate Checks:")
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    print(f"Min date: {df['InvoiceDate'].min()}")
    print(f"Max date: {df['InvoiceDate'].max()}")
    print(f"Missing/Invalid dates: {df['InvoiceDate'].isnull().sum()}")
    print(f"Future dates: {df[df['InvoiceDate'] > pd.Timestamp.now()].shape[0]}")

    print("\nMetadata StockCodes:")
    system_codes = ["POST", "ADJUST", "CHECK"]
    flagged = df["StockCode"].astype(str).str.upper().isin(system_codes)
    print(f"System/Metadata StockCodes found: {flagged.sum()}")

    print("\nCustomer IDs with Multiple Countries:")
    multi_country = df.groupby("Customer ID")["Country"].nunique()
    print(f"Customers in >1 country: {multi_country[multi_country > 1].shape[0]}")

    print("\nUnique Counts Summary:")
    print({
        "Unique Invoices": df["Invoice"].nunique(),
        "Unique Customers": df["Customer ID"].nunique(),
        "Unique Products": df["StockCode"].nunique(),
        "Unique Countries": df["Country"].nunique()
    })

if __name__ == "__main__":
    profile_raw_data("C:/Users/Allthingdata/DE_project/Betsson/Approach1/data/raw/transactions.csv")
