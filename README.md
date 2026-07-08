## Requirements
- Python 3.11 (PySpark 3.5.7 has a known worker-handshake incompatibility with Python 3.12+ on Windows)
- Docker Desktop with WSL2 backend
- `C:\hadoop\bin` must be added to PATH (not just HADOOP_HOME) or Spark Structured Streaming checkpointing fails with `UnsatisfiedLinkError: NativeIO$Windows.access0`

## dbt-spark + Iceberg setup notes
- dbt-spark session mode requires `dbt-spark[session]` extras plus manual install of `PyHive`, `thrift`, `thrift-sasl` (extras install doesn't pull these automatically)
- Iceberg catalog config for dbt must go in `profiles.yml` under `server_side_parameters`, not `sources.yml`
- The Iceberg catalog must be named `spark_catalog` (overriding Spark's built-in default), not a custom name like `local` — dbt-spark resolves unqualified source references through the default catalog only
- All dbt models targeting Iceberg tables must use `{{ config(materialized='table') }}` — Iceberg's Spark catalog does not support SQL views