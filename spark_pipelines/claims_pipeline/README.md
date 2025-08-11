# claims_pipeline (PySpark)

This pipeline reads a dataset from the local `data source/` folder using PySpark,
performs basic data quality checks, and writes curated Parquet outputs under `output/`.

## Run
python3 spark_pipeline.py --input "data source/claims_enhanced.csv" --output "output"

## Quality checks
- Row/column counts
- Null counts per column
- Duplicate row count
- Basic schema print