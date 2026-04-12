import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    from load_postgres import write_df, read_parquet

    return read_parquet, write_df


@app.cell
def _(read_parquet, write_df):
    df = read_parquet("../data/processed/surveys.parquet")
    dsn = "postgresql://admin:admin@localhost:5433/vote_surveys"
    inserted = write_df(df, dsn, "staging_votes")
    inserted
    return


if __name__ == "__main__":
    app.run()
