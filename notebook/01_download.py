import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    from pathlib import Path
    from download import download_csv

    return Path, download_csv


@app.cell
def _(Path, download_csv):
    for vote in range(636,682):
        url = f"https://swissvotes.ch/vote/{str(vote)}.00/nachbefragung.csv"
        out = Path(f"../data/raw/vote_{str(vote)}.csv")
        path = download_csv(url, out)
        path
    return


if __name__ == "__main__":
    app.run()
