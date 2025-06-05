import pandas as pd

def build_star_schema(df: pd.DataFrame) -> dict:
    """
    Converts cleaned invoice data into a star schema:
    - dim_product
    - dim_customer
    - dim_date
    - fact_invoice

    Args:
        df (pd.DataFrame): Cleaned invoice data

    Returns:
        dict: Dictionary containing each dimension and fact DataFrame
    """
    df = df.copy()
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

    # ----------------------
    # dim_product
    # ----------------------
    dim_product = df[["StockCode", "Description"]].drop_duplicates().reset_index(drop=True)
    dim_product["product_id"] = dim_product.index + 1
    dim_product = dim_product[["StockCode", "Description", "product_id"]]

    # ----------------------
    # dim_customer
    # ----------------------
    dim_customer = df[["Customer ID", "Country"]].drop_duplicates().reset_index(drop=True)
    dim_customer["customer_key"] = dim_customer.index + 1
    dim_customer = dim_customer[["Customer ID", "Country", "customer_key"]]

    # ----------------------
    # dim_date
    # ----------------------
    dim_date = df[["InvoiceDate"]].drop_duplicates().copy()
    dim_date["date_key"] = dim_date["InvoiceDate"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["InvoiceDate"].dt.year
    dim_date["month"] = dim_date["InvoiceDate"].dt.month
    dim_date["day"] = dim_date["InvoiceDate"].dt.day
    dim_date["weekday"] = dim_date["InvoiceDate"].dt.day_name()
    dim_date = dim_date.sort_values("InvoiceDate").reset_index(drop=True)

    # ----------------------
    # Mappings for FK assignment
    # ----------------------
    product_map = dim_product.set_index(["StockCode", "Description"])["product_id"]
    customer_map = dim_customer.set_index(["Customer ID", "Country"])["customer_key"]
    date_map = dim_date.set_index("InvoiceDate")["date_key"]

    # ----------------------
    # fact_invoice
    # ----------------------
    fact_invoice = df.copy()
    fact_invoice["product_id"] = df.set_index(["StockCode", "Description"]).index.map(product_map)
    fact_invoice["customer_key"] = df.set_index(["Customer ID", "Country"]).index.map(customer_map)
    fact_invoice["date_key"] = df["InvoiceDate"].map(date_map)

    fact_invoice = fact_invoice[[
        "Invoice", "product_id", "customer_key", "date_key",
        "Quantity", "Price", "TotalPrice"
    ]]

    # Add this to flag mismatched TotalPrice values for further inspection
    mismatched = fact_invoice[
        (fact_invoice["Price"] > 0) &
        (fact_invoice["TotalPrice"].round(2) != (fact_invoice["Quantity"] * fact_invoice["Price"]).round(2))
    ]
    mismatched.to_csv("output/mismatched_totalprice_rows.csv", index=False)

    return {
        "dim_product": dim_product,
        "dim_customer": dim_customer,
        "dim_date": dim_date,
        "fact_invoice": fact_invoice
    }
