import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


REQUIRED_COLUMNS = [
    "event_id",
    "user_id",
    "event_timestamp",
    "event_type",
    "page_url",
    "product_id",
]

ALLOWED_EVENT_TYPES = {"page_view", "product_view", "add_to_cart", "purchase"}


def _load_raw_json(raw_path: str) -> pd.DataFrame:
    files = sorted(Path(raw_path).glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No JSON files found in {raw_path}")

    records: list[dict] = []
    for file_path in files:
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    if not records:
        raise ValueError("No JSON objects found in source files.")
    return pd.DataFrame(records)


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    out = df.copy()
    # Cast and clean types. We ensure `event_timestamp` is timezone-naive UTC and uses microsecond precision
    # so it lands as `timestamp[us]` in Parquet.
    out["event_id"] = out["event_id"].astype("string")
    out["user_id"] = pd.to_numeric(out["user_id"], errors="coerce")
    out["event_timestamp"] = pd.to_datetime(out["event_timestamp"], errors="coerce", utc=True)
    out["event_timestamp"] = out["event_timestamp"].dt.tz_convert(None).dt.as_unit("us")
    out["event_type"] = out["event_type"].astype("string")
    out["page_url"] = out["page_url"].astype("string")
    # Keep `product_id` as nullable integer so Parquet schema is `int64` with nullability.
    out["product_id"] = pd.to_numeric(out["product_id"], errors="coerce").astype("Int64")
    out["event_date"] = out["event_timestamp"].dt.date

    out = out[out["event_type"].isin(ALLOWED_EVENT_TYPES)]

    not_null_cols = ["event_id", "user_id", "event_timestamp", "event_type", "event_date"]
    out = out.dropna(subset=not_null_cols)

    # Now that critical columns are guaranteed non-null, use non-nullable pandas dtypes so pyarrow infers
    # `nullable=False` for those fields.
    out["event_id"] = out["event_id"].astype(str)
    out["user_id"] = out["user_id"].astype("int64")
    out["event_type"] = out["event_type"].astype(str)
    out["event_timestamp"] = pd.to_datetime(out["event_timestamp"], utc=False).dt.as_unit("us")
    return out


def _write_partitioned_parquet(df: pd.DataFrame, dest_path: str) -> None:
    Path(dest_path).mkdir(parents=True, exist_ok=True)

    # Explicit schema so pyarrow/parquet preserves nullability exactly as required.
    schema = pa.schema(
        [
            pa.field("event_id", pa.string(), nullable=False),
            pa.field("user_id", pa.int64(), nullable=False),
            pa.field("event_timestamp", pa.timestamp("us"), nullable=False),
            pa.field("page_url", pa.string(), nullable=True),
            pa.field("product_id", pa.int64(), nullable=True),
            pa.field("event_date", pa.date32(), nullable=False),
            pa.field("event_type", pa.string(), nullable=False),
        ]
    )

    # Ensure expected logical dtypes before converting to Arrow.
    df = df.copy()
    df["event_id"] = df["event_id"].astype(str)
    df["user_id"] = df["user_id"].astype("int64")
    df["event_type"] = df["event_type"].astype(str)
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=dest_path,
        partition_cols=["event_date", "event_type"],
    )


def main() -> None:
    source = os.getenv("RAW_INPUT_PATH", "output/raw")
    destination = os.getenv("PROCESSED_OUTPUT_PATH", "output/processed")

    raw_df = _load_raw_json(source)
    transformed = _transform(raw_df)
    _write_partitioned_parquet(transformed, destination)
    print(f"Transformed {len(transformed)} records into partitioned parquet at {destination}")


if __name__ == "__main__":
    main()
