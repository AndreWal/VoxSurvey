from psycopg import connect, sql
import polars as pl
from pathlib import Path

def write_df(df: pl.DataFrame, dsn: str, table: str) -> int:
    rows = df.to_dicts()
    if not rows:
        return 0
    cols = list(rows[0].keys())

    type_map = {pl.Int32: "INTEGER", pl.Int64: "BIGINT", pl.Utf8: "TEXT", pl.Float64: "FLOAT"}
    ddl_cols = sql.SQL(", ").join(
        sql.SQL("{} {}").format(
            sql.Identifier(c),
            sql.SQL(type_map.get(df[c].dtype, "TEXT")),
        )
        for c in cols
    )
    create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table), ddl_cols
    )

    col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(cols))
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table), col_ids, placeholders
    )

    drop_sql = sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table))

    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(drop_sql)
            cur.execute(create_sql)
            cur.executemany(insert_sql, [[row[c] for c in cols] for row in rows])
        conn.commit()
    return len(rows)


def read_parquet(path: str | Path) -> pl.DataFrame:
    return pl.read_parquet(path)