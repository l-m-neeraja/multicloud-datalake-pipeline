import os
from pathlib import Path

import great_expectations as gx
import pandas as pd
import pyarrow.dataset as ds
from great_expectations.core.batch import RuntimeBatchRequest


def load_processed_dataset(path: str) -> pd.DataFrame:
    # Read partitioned parquet into a single pandas DataFrame so GE can validate it.
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    table = dataset.to_table()
    return table.to_pandas()


def build_success_result_index_exists(report_path: Path) -> bool:
    # GE can take a moment to write the report; we simply verify existence after build.
    return report_path.exists() and report_path.stat().st_size > 0


def main() -> int:
    processed_path = os.getenv("PROCESSED_OUTPUT_PATH", "output/processed")
    max_future_minutes = int(os.getenv("VALIDATION_MAX_FUTURE_MINUTES", "5"))
    _ = max_future_minutes  # kept for parity with the project contract

    # Great Expectations DataContext configured for local file stores.
    context = gx.get_context(context_root_dir="great_expectations")

    df = load_processed_dataset(processed_path)

    batch_request = RuntimeBatchRequest(
        datasource_name="clickstream_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="processed_clickstream",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default"},
    )

    checkpoint = context.add_or_update_checkpoint(
        name="clickstream_processed_checkpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": "clickstream.processed",
            }
        ],
    )

    print("Running Great Expectations checkpoint: clickstream_processed_checkpoint")
    checkpoint_result = checkpoint.run()

    # Generate HTML docs. The contract expects the report at this exact path.
    context.build_data_docs(site_names=["local_site"])

    report_path = Path("great_expectations/uncommitted/data_docs/local_site/index.html")
    if build_success_result_index_exists(report_path):
        success = None
        if checkpoint_result is not None:
            success = getattr(checkpoint_result, "success", None)
            if success is None and isinstance(checkpoint_result, dict):
                success = checkpoint_result.get("success")
        return 0 if success else 1

    # If report not created, treat as failure.
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
