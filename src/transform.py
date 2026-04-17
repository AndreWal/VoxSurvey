import os
import re

import openpyxl
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

vote_year: dict[range, int] = {
    range(636, 638): 2020,
    range(638, 651): 2021,
    range(651, 662): 2022,
    range(662, 665): 2023,
    range(665, 677): 2024,
    range(677, 681): 2025,
    range(681, 700): 2026,
}


def cal_age(df: pl.DataFrame, vote: int) -> pl.DataFrame:
    for rng, year in vote_year.items():
        if vote in rng:
            break
    else:
        raise ValueError(f"Unknown vote ID {vote} — add it to vote_year")

    return df.with_columns(
        (pl.lit(year) - pl.col("birthyearr").cast(pl.Int32)).alias("age")
    )

def stack_dfs(dfs: list[pl.DataFrame]) -> pl.DataFrame:
    return pl.concat(dfs, how="vertical_relaxed")

def rename_cols(df: pl.DataFrame, col_mapping: dict[str, str]) -> pl.DataFrame:
    return df.rename(col_mapping)

def add_vote_id(df: pl.DataFrame, vote_id: int) -> pl.DataFrame:
    return df.with_columns(pl.lit(vote_id).alias("vote_id"))

def replace_invalid_with_null_eight(
    df: pl.DataFrame,
    cols: list[str] = ["gender", "polint","vote"],
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

def replace_invalid_with_null_nine_eight(
    df: pl.DataFrame,
    cols: list[str] = ["lrsp"],
    invalid_code: int = 98,
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


def replace_invalid_with_null_large(
    df: pl.DataFrame,
    cols: list[str] = ["vote"],
    invalid_code: int = 99999998,
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


def read_codebook_votes(path: str) -> list[tuple[str, str]]:
    """Read vote column names and descriptions from a codebook xlsx file.

    Returns a list of (column_name, description) tuples, e.g.
    [('vote1', 'Wie haben Sie bei ...'), ('vote2', 'Wie haben Sie bei ...')].
    """
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    entries: list[tuple[str, str]] = []
    for row in ws.iter_rows(min_row=1, values_only=True):
        # Standard format: col B = variable name, col C = German description
        if (
            len(row) >= 3
            and row[1]
            and isinstance(row[1], str)
            and re.match(r"^vote\d*$", row[1].strip().lower())
        ):
            entries.append((row[1].strip().lower(), str(row[2]) if row[2] else ""))
        # Alternate format (e.g. vote_677): col A = variable, col D = German
        elif (
            len(row) >= 4
            and row[0]
            and isinstance(row[0], str)
            and re.match(r"^vote\d*$", row[0].strip().lower())
        ):
            entries.append((row[0].strip().lower(), str(row[3]) if row[3] else ""))
    wb.close()
    return entries


def build_vote_column_mapping(
    raw_dir: str, vote_ids: range
) -> dict[int, tuple[str, str]]:
    """Map each vote_id to its (vote_column_name, vote_description).

    Groups consecutive vote_ids that share identical codebook descriptions,
    then assigns each vote_id to its positional vote column within the group.
    E.g. if 636 and 637 share the same two vote descriptions, 636 → vote1
    and 637 → vote2.
    """
    id_entries: dict[int, list[tuple[str, str]]] = {}
    id_descs: dict[int, tuple[str, ...]] = {}
    for vid in vote_ids:
        path = os.path.join(raw_dir, f"vote_{vid}_codebuch.xlsx")
        entries = read_codebook_votes(path)
        id_entries[vid] = entries
        id_descs[vid] = tuple(desc for _, desc in entries)

    # Group consecutive IDs with identical descriptions
    ids = list(vote_ids)
    groups: list[list[int]] = []
    current_group = [ids[0]]
    for vid in ids[1:]:
        if id_descs[vid] == id_descs[vid - 1]:
            current_group.append(vid)
        else:
            groups.append(current_group)
            current_group = [vid]
    groups.append(current_group)

    # Assign vote column by position within group
    mapping: dict[int, tuple[str, str]] = {}
    for group in groups:
        entries = id_entries[group[0]]
        for idx, vid in enumerate(group):
            if idx < len(entries):
                col_name, desc = entries[idx]
            else:
                # More files than codebook entries — fall back to positional name
                col_name = f"vote{idx + 1}"
                desc = ""
            mapping[vid] = (col_name, desc)

    return mapping