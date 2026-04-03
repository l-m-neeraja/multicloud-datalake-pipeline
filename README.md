# Multi-Cloud Data Lake Pipeline (GCS -> Parquet -> S3 -> Redshift Spectrum)

This project implements an end-to-end clickstream pipeline across GCP and AWS:

1. Generate raw clickstream JSON events.
2. Transform raw events into partitioned Parquet (`event_date`, `event_type`).
3. Validate processed data quality.
4. Sync processed data from GCS to S3 using `rclone`.
5. Query data in Redshift Spectrum and expose a funnel-analysis view.

## Project Structure

- `docker-compose.yml`, `Dockerfile`: local reproducible runtime.
- `scripts/generate_events.py`: generates >= 50,000 events to `output/raw/`.
- `scripts/transform_data.py`: transforms JSON to partitioned Parquet in `output/processed/`.
- `scripts/run_validation.sh`: runs validation checkpoint.
- `scripts/sync_to_s3.sh`: syncs data with `rclone sync`.
- `great_expectations/`: expectation suite and checkpoint config.
- `sql/create_external_table.sql`: Redshift Spectrum schema/table DDL.
- `sql/create_funnel_view.sql`: funnel analysis view DDL.
- `DESIGN_DOC.md`: architecture/design decisions and scaling discussion.

## Prerequisites

- Docker + Docker Compose.
- GCP account, GCS bucket, service account key JSON.
- AWS account, S3 bucket, Redshift Serverless, IAM role for Spectrum.

## Setup

1. Copy env template:
   - `cp .env.example .env` (Linux/macOS) or `copy .env.example .env` (Windows cmd)
2. Fill `.env` with your real values.
3. Build runtime image:
   - `docker-compose build`

## Run Pipeline (Local Test)

1. Generate events:
   - `docker-compose run --rm app python scripts/generate_events.py`
2. Transform to partitioned Parquet:
   - `docker-compose run --rm app python scripts/transform_data.py`
3. Run data validation:
   - `docker-compose run --rm app bash scripts/run_validation.sh`
4. Validation report:
   - `great_expectations/uncommitted/data_docs/local_site/index.html`

## Run Cross-Cloud Sync

Configure `rclone` remotes or use cloud paths in env vars:

- `GCS_PROCESSED_PATH`
- `S3_LANDING_PATH`

Then run:

- `docker-compose run --rm app bash scripts/sync_to_s3.sh`

## Redshift Spectrum Setup

1. Open your SQL client connected to Redshift Serverless.
2. Run:
   - `sql/create_external_table.sql`
3. Add partitions (manual `ALTER TABLE ... ADD PARTITION ...` or Glue crawler).
4. Run:
   - `sql/create_funnel_view.sql`
