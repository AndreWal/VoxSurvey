import marimo

app = marimo.App()

@app.cell
def __():
    from dd_survey_pipeline.transform import read_csv, normalize
    return read_csv, normalize

@app.cell
def __(read_csv, normalize):
    df = read_csv("data/raw/file.csv")
    out = normalize(df)
    out.head()
    return

if __name__ == "__main__":
    app.run()