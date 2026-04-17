import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    from pathlib import Path
    from download import download_csv, download_xlsx

    return Path, download_csv, download_xlsx


@app.cell
def _(Path, download_csv):
    for vote in range(636, 682):
        url = f"https://swissvotes.ch/vote/{str(vote)}.00/nachbefragung.csv"
        out = Path(f"../data/raw/vote_{str(vote)}.csv")

        if out.exists():
            print(f"Already exists locally: {out}")
            continue

        path = download_csv(url, out)
        print(f"Downloaded: {path}")
    return


@app.cell
def _(Path, download_xlsx):
    for vote2 in range(636, 682):
        url2 = f"https://swissvotes.ch/vote/{str(vote2)}.00/nachbefragung-codebuch-de.xlsx"
        out2 = Path(f"../data/raw/vote_{str(vote2)}_codebuch.xlsx")

        if out2.exists():
            print(f"Already exists locally: {out2}")
            continue

        path2 = download_xlsx(url2, out2)
        print(f"Downloaded: {path2}")
    return


@app.cell
def _():
    print("All files downloaded.")
    return


if __name__ == "__main__":
    app.run()
