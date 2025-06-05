import pandas as pd

def load_data(file_path: str) -> pd.DataFrame:
    """
    Load invoice data from a CSV file using fallback encoding.
    Strips column names to remove leading/trailing whitespace.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: Loaded DataFrame with clean column names.
    """
    try:
        df = pd.read_csv(file_path, encoding="utf-8", low_memory=False, dtype={"Customer ID": str})
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding="ISO-8859-1", low_memory=False, dtype={"Customer ID": str})


    # Clean column names
    df.columns = df.columns.str.strip()

    return df
