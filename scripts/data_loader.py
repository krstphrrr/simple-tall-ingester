import polars as pl

def load_csv(file_path: str) -> pl.DataFrame:
    try:
        df = pl.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None
