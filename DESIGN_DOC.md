# Design Document: Multi-Cloud Clickstream Data Lake

## Architecture Overview

The pipeline separates ingestion, processing, quality, transport, and analytics:

1. **Ingestion (GCP landing zone)**  
   Raw clickstream JSON lands in `gs://<bucket>/raw/`.
2. **Transformation (Python + Parquet)**  
   Raw JSON is normalized and converted into partitioned Parquet at `processed/event_date=.../event_type=...`.
3. **Data quality gate**  
   A validation checkpoint checks schema and business rules before promotion.
4. **Cross-cloud synchronization**  
   `rclone sync` mirrors processed data from GCS to S3.
5. **Analytics serving layer (AWS)**  
   Redshift Spectrum external table reads Parquet in S3. A view exposes funnel KPIs.

## Storage Tier Choices

### Why GCS for landing/processing

- Application-side data often already lands in GCP.
- GCS is cost-effective for durable object storage and raw-history retention.
- Keeping raw and processed zones in one bucket simplifies lineage and replay.

### Why S3 for Redshift Spectrum querying

- Spectrum natively reads external files from S3.
- Avoids loading every event into Redshift managed storage.
- Supports decoupled storage/compute and cheaper long-term lake retention.

### Cost and performance implications

- **Cost**: object storage is cheap, but cross-cloud transfer and repeated scans can add cost.
- **Performance**: Parquet + partitioning dramatically reduces bytes scanned.
- **Operational trade-off**: sync introduces extra moving parts and consistency timing.

## Data Format Choice: Parquet

### Why Parquet

- Columnar layout improves analytical scan performance.
- Better compression than row-oriented JSON.
- Schema and types are preserved (timestamps, integers).

### Trade-offs vs JSON / Avro

- **vs JSON**: Parquet is less human-readable but far better for analytics.
- **vs Avro**: Avro is strong for row-wise streaming/exchange; Parquet is usually better for SQL scans.
- Parquet files require careful partition/file-size management for optimal performance.

## Scaling to 100x (50k/day -> 5M/day)

At 100x scale, bottlenecks appear in three places:

1. **Transformation script limits**
   - Current single-process Pandas flow can become memory/CPU bound.
   - Move to distributed processing: Spark (Dataproc/EMR) or Dataflow/Beam.
   - Introduce incremental micro-batch processing and idempotent writes.

2. **Cross-cloud sync throughput/cost**
   - Large sync windows increase transfer time and egress cost.
   - Add scheduling, parallel transfer tuning, and daily/hourly partition-level syncs.
   - Consider event-driven transfer architecture or dual-write strategy where feasible.

3. **Query performance in Spectrum**
   - Too many small Parquet files hurt performance.
   - Implement compaction strategy and target optimal file sizes.
   - Add partition projection or Glue crawler automation and strict query filters.
   - Materialize frequently used aggregates in Redshift tables if latency SLAs tighten.

## Reliability and Operations

- Add orchestration with Airflow/Prefect for retries and dependency management.
- Track validation outcomes and publish data quality metrics.
- Add data contracts and schema evolution strategy.
- Implement monitoring and alerting for sync failures and partition freshness.
