import polars as pl
from transform import _detect_separator, cal_age, replace_invalid_with_null_eight, replace_invalid_with_null_nine_eight, normalize


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


def test_replace_invalid_with_null_eight():
    df = pl.DataFrame({"gender": [1, 8, 3], "polint": [2, 4, 8]})
    result = replace_invalid_with_null_eight(df, cols=["gender", "polint"])
    assert result["gender"].to_list() == [1, None, 3]
    assert result["polint"].to_list() == [2, 4, None]


def test_replace_invalid_with_null_nine_eight():
    df = pl.DataFrame({"lrsp": [5, 98, 3]})
    result = replace_invalid_with_null_nine_eight(df)
    assert result["lrsp"].to_list() == [5, None, 3]