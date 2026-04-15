import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import polars as pl
    from schemas import SurveyRow

    return SurveyRow, pl


@app.cell
def _(SurveyRow, pl):
    df = pl.read_parquet("../data/processed/surveys.parquet")
    records = df.to_dicts()
    valid = [SurveyRow.model_validate(r) for r in records]
    print("All records are valid!")
    return


if __name__ == "__main__":
    app.run()
