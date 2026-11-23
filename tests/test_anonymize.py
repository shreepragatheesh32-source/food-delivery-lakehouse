import pandas as pd
import pytest

import anonymize as an


def source_frame() -> pd.DataFrame:
    row = {column: "" for column in an.TIMESTAMP_COLUMNS}
    row.update(
        {
            "Created At": "01-01-2023 10:00",
            "Started Time": "05-03-2025 18:00",
            "Checkin Time (WP2)": "05-03-2025 18:12",
            "External Id": "9999-1000",
            "Run Id": "555001",
            "Fleet Name": "Partner Carrier",
            "Team Name": "12 Example Bistro",
            "Driver Name": "Q",
            "Tag": "Example Bistro",
            "Status": "Done",
            "Billable by Vendorco": "1",
            "Day of Week": "Wednesday",
        }
    )
    copy = {**row, "Created At": "07-07-2023 11:00"}
    other = {
        **row,
        "External Id": "9999-2000",
        "Run Id": "",
        "Fleet Name": "",
        "Driver Name": "R",
        "Tag": "Service",
    }
    return pd.DataFrame([row, copy, other]).astype(str)


def secret_values() -> dict:
    return {"salt": "test-salt", "week_offset": -60, "maps": {"run": {}, "driver": {}, "fleet": {}}}


def test_duplicates_survive_anonymization():
    output = an.anonymize(source_frame(), secret_values())
    key = [c for c in output.columns if c != "Created At"]
    assert len(output) == 3
    assert len(output[key].drop_duplicates()) == 2


def test_identifiers_and_vendor_column_are_replaced():
    output = an.anonymize(source_frame(), secret_values())
    assert "Billable by Vendorco" not in output
    assert set(output["Team Name"]) == {"STORE_01"}
    assert set(output["Tag"]) == {"Standard", "Service"}
    assert output.loc[0, "External Id"].startswith("ORD_")
    assert output.loc[0, "Driver Name"].startswith("DRV_")


def test_blanks_stay_blank():
    output = an.anonymize(source_frame(), secret_values())
    assert output.loc[2, "Run Id"] == ""
    assert output.loc[2, "Fleet Name"] == ""
    assert output.loc[0, "No Later Than"] == ""


def test_time_shift_keeps_weekday_and_duration():
    source = source_frame()
    output = an.anonymize(source, secret_values())
    start = pd.to_datetime(output["Started Time"], format=an.TIMESTAMP_FORMAT)
    end = pd.to_datetime(output["Checkin Time (WP2)"], format=an.TIMESTAMP_FORMAT)
    assert (start.dt.day_name() == source["Day of Week"]).all()
    assert ((end - start) == pd.Timedelta(minutes=12)).all()


def test_mapping_is_stable_across_runs():
    values = secret_values()
    first = an.anonymize(source_frame(), values)
    second = an.anonymize(source_frame(), values)
    pd.testing.assert_frame_equal(first, second)


def test_validate_passes_clean_output():
    source = source_frame()
    values = secret_values()
    report = an.validate(source, an.anonymize(source, values), values)
    assert report == {"rows": 3, "distinct_deliveries": 2, "columns": len(source.columns) - 1}


def test_validate_catches_leaked_brand():
    source = source_frame()
    values = secret_values()
    output = an.anonymize(source, values)
    output["Status"] = "Example Bistro"
    with pytest.raises(ValueError, match="sensitive term"):
        an.validate(source, output, values)


def test_sample_keeps_whole_delivery_groups():
    values = secret_values()
    output = an.anonymize(source_frame(), values)
    sample = an.make_sample(output, values["salt"], deliveries=2)
    assert len(sample) == 3
