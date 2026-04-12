import polars as pl
import pandas as pd


def _detect_separator(path: str) -> str:
    with open(path, "r", encoding="latin-1", errors="replace") as f:
        header = f.readline()
    return ";" if header.count(";") > header.count(",") else ","


def read_csv(path: str) -> pl.DataFrame:
    sep = _detect_separator(path)
    try:
        return pl.read_csv(
            path,
            encoding="utf8-lossy",
            separator=sep,
            truncate_ragged_lines=True,
            ignore_errors=True,
        )
    except Exception:
        # Some Swissvotes exports contain malformed quoted rows and legacy encoding.
        # Pandas' python engine can skip bad lines and still recover the dataset.
        df_pd = pd.read_csv(
            path,
            encoding="latin-1",
            sep=sep,
            engine="python",
            on_bad_lines="skip",
            dtype=str,
            keep_default_na=False,
        )
        return pl.DataFrame(df_pd.to_dict(orient="list"), strict=False)

def normalize(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({c: c.strip().lower() for c in df.columns})

def select_cols(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    return df.select(cols)

def cal_age(df: pl.DataFrame, vote: int) -> pl.DataFrame:
    if vote >= 681:
        year = 2026
    elif vote > 676:
        year = 2025
    elif vote > 664:
        year = 2024
    elif vote > 661:
        year = 2023
    elif vote > 650:
        year = 2022
    elif vote > 637:
        year = 2021
    else:
        year = 2020

    return df.with_columns(
        (pl.lit(year) - pl.col("birthyearr").cast(pl.Int32)).alias("age")
    )

def stack_dfs(dfs: list[pl.DataFrame]) -> pl.DataFrame:
    return pl.concat(dfs, how="vertical_relaxed")

def rename_cols(df: pl.DataFrame, col_mapping: dict[str, str]) -> pl.DataFrame:
    return df.rename(col_mapping)

def add_vote_id(df: pl.DataFrame, vote_id: int) -> pl.DataFrame:
    return df.with_columns(pl.lit(vote_id).alias("vote_id"))

def replace_invalid_with_null(
    df: pl.DataFrame,
    cols: list[str] = ["gender", "polint"],
    invalid_code: int = 8,
) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.when(pl.col(c).cast(pl.Int32, strict=False) == invalid_code)
            .then(None)
            .otherwise(pl.col(c).cast(pl.Int32, strict=False))
            .cast(pl.Int32)
            .alias(c)
            for c in cols
        ]
    )