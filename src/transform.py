import polars as pl

def read_csv(path: str) -> pl.DataFrame:
    return pl.read_csv(path)

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({c: c.strip().lower() for c in df.columns})