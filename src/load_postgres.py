import psycopg
import polars as pl

def write_df(df: pl.DataFrame, dsn: str, table: str) -> int:
    rows = df.to_dicts()
    if not rows:
        return 0
    cols = list(rows[0].keys())

    type_map = {pl.Int32: "INTEGER", pl.Int64: "BIGINT", pl.Utf8: "TEXT", pl.Float64: "FLOAT"}
    ddl_cols = ", ".join(
        f"{c} {type_map.get(df[c].dtype, 'TEXT')}"
        for c in cols
    )
    create_sql = f"CREATE TABLE IF NOT EXISTS {table} ({ddl_cols})"


    values_sql = ", ".join(["%s"] * len(cols))
    insert_sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({values_sql})"
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            for row in rows:
                cur.execute(insert_sql, [row[c] for c in cols])
        conn.commit()
    return len(rows)

def read_parquet(df: pl.DataFrame) -> pl.DataFrame:
    return pl.read_parquet(df)