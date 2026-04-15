import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    from transform import read_csv, normalize, select_cols, cal_age, stack_dfs, rename_cols, add_vote_id, replace_invalid_with_null_eight, replace_invalid_with_null_nine_eight

    return (
        add_vote_id,
        cal_age,
        normalize,
        read_csv,
        rename_cols,
        replace_invalid_with_null_eight,
        replace_invalid_with_null_nine_eight,
        select_cols,
        stack_dfs,
    )


@app.cell
def _():
    cols = ["age", "s11","polint", "lrsp"]
    col_mapping = {
        "s11": "gender",
    }
    dat = []
    return col_mapping, cols, dat


@app.cell
def _(
    add_vote_id,
    cal_age,
    col_mapping,
    cols,
    dat,
    normalize,
    read_csv,
    rename_cols,
    replace_invalid_with_null_eight,
    replace_invalid_with_null_nine_eight,
    select_cols,
):
    for i in range(636,682):
        df = read_csv(f"../data/raw/vote_{i}.csv")
        out = normalize(df)
        out2 = cal_age(out, i)
        out3 = select_cols(out2, cols)
        out4 = rename_cols(out3, col_mapping)
        out5 = replace_invalid_with_null_eight(out4)
        out6 = replace_invalid_with_null_nine_eight(out5)
        subdf = add_vote_id(out6, i)
        dat.append(subdf)
    return


@app.cell
def _(dat, stack_dfs):
    all_df = stack_dfs(dat)
    all_df.write_parquet("../data/processed/surveys.parquet")
    print(f"Saved {len(all_df)} rows to data/processed/surveys.parquet successfully.")
    return (all_df,)


@app.cell
def _(all_df):
    all_df
    return


if __name__ == "__main__":
    app.run()
