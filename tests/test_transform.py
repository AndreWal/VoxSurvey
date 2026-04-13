import polars as pl
from transform import _detect_separator, cal_age, replace_invalid_with_null, normalize


def test_detect_separator_semicolon(tmp_path):
    f = tmp_path / "semi.csv"
    f.write_text("a;b;c\n1;2;3\n")
    assert _detect_separator(str(f)) == ";"


def test_detect_separator_comma(tmp_path):
    f = tmp_path / "comma.csv"
    f.write_text("a,b,c\n1,2,3\n")
    assert _detect_separator(str(f)) == ","


def test_cal_age_computes_correctly():
    df = pl.DataFrame({"birthyearr": [1990, 2000]})
    result = cal_age(df, 670)  # year 2024
    assert result["age"].to_list() == [34, 24]


def test_cal_age_unknown_vote():
    df = pl.DataFrame({"birthyearr": [1990]})
    try:
        cal_age(df, 9999)
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


def test_replace_invalid_with_null():
    df = pl.DataFrame({"gender": [1, 8, 3], "polint": [2, 4, 8]})
    result = replace_invalid_with_null(df)
    assert result["gender"].to_list() == [1, None, 3]
    assert result["polint"].to_list() == [2, 4, None]


def test_normalize_lowercases_columns():
    df = pl.DataFrame({"Name": [1], " AGE ": [2]})
    result = normalize(df)
    assert result.columns == ["name", "age"]