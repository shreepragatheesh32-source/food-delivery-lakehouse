"""Create an anonymized copy of the delivery export before Bronze ingestion.

Replacements are deterministic, so duplicated source rows remain duplicates and
can be removed later in the Silver layer. Secrets and mappings are stored
outside the repository.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import re
import secrets
from pathlib import Path

import pandas as pd

TIMESTAMP_FORMAT = "%d-%m-%Y %H:%M"
TIMESTAMP_COLUMNS = [
    "Created At",
    "Last Assigned Time",
    "Started Time",
    "Checkin Time (WP1)",
    "Checkout Time (WP1)",
    "Scheduled At",
    "Checkin Time (WP2)",
    "Checkout Time (WP2)",
    "No Earlier Than",
    "No Later Than",
    "First Attempt Promise No Earlier Than",
    "First Attempt Promise No Later Than",
]
VENDOR_COLUMN_PREFIX = "Billable by"
SAMPLE_DELIVERIES = 25


def read_source(path: Path) -> pd.DataFrame:
    """Read every value as text so blanks and formats are preserved."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def load_secrets(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {
        "salt": secrets.token_hex(32),
        # Whole weeks preserve weekdays. The range moves dates 1 to 3 years back.
        "week_offset": -(52 + secrets.randbelow(105)),
        "maps": {"run": {}, "driver": {}, "fleet": {}},
    }


def save_secrets(path: Path, values: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values, indent=2, sort_keys=True), encoding="utf-8")


def hash_order_id(value: str, salt: str) -> str:
    if value == "":
        return ""
    digest = hmac.new(salt.encode(), value.encode(), hashlib.sha256).hexdigest()
    return f"ORD_{digest[:16]}"


def fleet_label(number: int) -> str:
    letters = ""
    while number > 0:
        number, remainder = divmod(number - 1, 26)
        letters = chr(65 + remainder) + letters
    return f"FLEET_{letters}"


def extend_mapping(series: pd.Series, mapping: dict, label) -> pd.Series:
    """Map values to labels, assigning new labels in random order."""
    new_values = [v for v in series.unique() if v != "" and v not in mapping]
    secrets.SystemRandom().shuffle(new_values)
    for value in new_values:
        mapping[value] = label(len(mapping) + 1)
    return series.map(lambda v: "" if v == "" else mapping[v])


def shift_timestamps(series: pd.Series, weeks: int) -> pd.Series:
    parsed = pd.to_datetime(series.where(series != ""), format=TIMESTAMP_FORMAT)
    shifted = parsed + pd.Timedelta(weeks=weeks)
    return shifted.dt.strftime(TIMESTAMP_FORMAT).astype(object).where(parsed.notna(), "")


def anonymize(source: pd.DataFrame, values: dict) -> pd.DataFrame:
    output = source.copy()
    maps = values["maps"]

    output["External Id"] = output["External Id"].map(
        lambda v: hash_order_id(v, values["salt"])
    )
    output["Run Id"] = extend_mapping(output["Run Id"], maps["run"], lambda n: f"RUN_{n:04d}")
    output["Driver Name"] = extend_mapping(
        output["Driver Name"], maps["driver"], lambda n: f"DRV_{n:02d}"
    )
    output["Fleet Name"] = extend_mapping(output["Fleet Name"], maps["fleet"], fleet_label)
    output["Team Name"] = "STORE_01"
    output["Tag"] = output["Tag"].map(lambda v: "Service" if v == "Service" else "Standard")

    for column in TIMESTAMP_COLUMNS:
        output[column] = shift_timestamps(output[column], values["week_offset"])

    vendor_columns = [c for c in output if c.startswith(VENDOR_COLUMN_PREFIX)]
    return output.drop(columns=vendor_columns)


def sensitive_terms(source: pd.DataFrame) -> set[str]:
    """Derive brand, store, and vendor words from the source file."""
    terms: set[str] = set()
    for column in ("Team Name", "Fleet Name"):
        terms.update(v for v in source[column].unique() if v)
    terms.update(v for v in source["Tag"].unique() if v and v != "Service")
    for value in list(terms):
        terms.update(w for w in re.findall(r"[A-Za-z]{4,}", value) if w.lower() != "fleet")
    for column in source:
        if column.startswith(VENDOR_COLUMN_PREFIX):
            terms.add(column.removeprefix(VENDOR_COLUMN_PREFIX).strip())
    return {t for t in terms if t.lower() not in {"owned", "service"}}


def validate(source: pd.DataFrame, output: pd.DataFrame, values: dict) -> dict:
    """Raise if the output lost analytical meaning or leaked source values."""
    if len(source) != len(output):
        raise ValueError("Row count changed.")

    shared = [c for c in output.columns if c != "Created At"]
    source_distinct = len(source[shared].drop_duplicates())
    output_distinct = len(output[shared].drop_duplicates())
    if source_distinct != output_distinct:
        raise ValueError("Distinct delivery count changed.")

    expected = pd.Timedelta(weeks=values["week_offset"])
    for column in TIMESTAMP_COLUMNS:
        before = pd.to_datetime(source[column].where(source[column] != ""), format=TIMESTAMP_FORMAT)
        after = pd.to_datetime(output[column].where(output[column] != ""), format=TIMESTAMP_FORMAT)
        difference = (after - before).dropna()
        if before.notna().sum() != after.notna().sum() or not (difference == expected).all():
            raise ValueError(f"Timestamp shift failed for {column}.")

    for column in ("External Id", "Run Id", "Driver Name", "Fleet Name"):
        overlap = (set(source[column]) - {""}) & set(output[column])
        if overlap:
            raise ValueError(f"Original values remain in {column}.")

    text = output.to_csv(index=False).lower()
    if any(term.lower() in text for term in sensitive_terms(source)):
        raise ValueError("A sensitive term remains in the output.")

    return {
        "rows": len(output),
        "distinct_deliveries": output_distinct,
        "columns": output.shape[1],
    }


def make_sample(output: pd.DataFrame, salt: str, deliveries: int = SAMPLE_DELIVERIES) -> pd.DataFrame:
    """Sample whole delivery groups so the published file still contains duplicates."""
    key = [c for c in output.columns if c != "Created At"]
    groups = output[key].drop_duplicates()
    seed = int(hashlib.sha256(salt.encode()).hexdigest()[:8], 16)
    chosen = groups.sample(n=min(deliveries, len(groups)), random_state=seed)
    return output.merge(chosen, on=key, how="inner")[output.columns.tolist()]


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, default=root / "data" / "anonymized" / "deliveries_anonymized.csv"
    )
    parser.add_argument(
        "--sample", type=Path, default=root / "data" / "sample" / "deliveries_sample.csv"
    )
    parser.add_argument(
        "--secrets",
        type=Path,
        default=root.parent / "private_do_not_commit" / "anonymization_secrets.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = read_source(args.input)
    values = load_secrets(args.secrets)
    output = anonymize(source, values)
    report = validate(source, output, values)
    sample = make_sample(output, values["salt"])

    save_secrets(args.secrets, values)
    for path, frame in ((args.output, output), (args.sample, sample)):
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False)

    print(json.dumps({**report, "sample_rows": len(sample)}, indent=2))


if __name__ == "__main__":
    main()
