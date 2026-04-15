import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    from load_postgres import write_df, read_parquet
    import os
    from dotenv import load_dotenv

    load_dotenv("../.env")

    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    dsn = f"postgresql://{user}:{password}@localhost:5433/{db}"
    return dsn, read_parquet, write_df


@app.cell
def _(dsn, read_parquet, write_df):
    df = read_parquet("../data/processed/surveys.parquet")
    inserted = write_df(df, dsn, "staging_votes")
    print(f"Inserted {inserted} records into the database successfully.")
    return


if __name__ == "__main__":
    app.run()
