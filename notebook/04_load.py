import marimo

app = marimo.App()

@app.cell
def __():
    from dd_survey_pipeline.transform import read_csv, normalize
    from dd_survey_pipeline.load_postgres import write_df
    return read_csv, normalize, write_df

@app.cell
def __(read_csv, normalize, write_df):
    df = normalize(read_csv("data/raw/file.csv"))
    dsn = "postgresql://admin:admin@localhost:5433/vote_surveys"
    inserted = write_df(df, dsn, "staging_votes")
    inserted
    return

if __name__ == "__main__":
    app.run()