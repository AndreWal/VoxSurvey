import psycopg
import polars as pl

def write_df(df: pl.DataFrame, dsn: str, table: str) -> int:
    rows = df.to_dicts()
    if not rows:
        return 0
    cols = list(rows[0].keys())
    values_sql = ", ".join(["%s"] * len(cols))
    insert_sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({values_sql})"
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(insert_sql, [row[c] for c in cols])
        conn.commit()
    return len(rows)