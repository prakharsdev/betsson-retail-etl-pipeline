import pandas as pd

def save_data(df: pd.DataFrame, path: str):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): The data to save.
        path (str): Destination file path.
    """
    df.to_csv(path, index=False)
