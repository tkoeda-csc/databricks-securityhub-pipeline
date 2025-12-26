# Databricks Security Hub Compliance Pipeline

AWS Security Hub compliance data processing pipeline using Databricks and S3.

## Overview

This project transforms AWS Security Hub findings into compliance metrics and exports them to S3 for consumption by external systems (Aurora MySQL, dashboards, etc.).

## 📁 Repository Structure

```
databricks-securityhub-pipeline/
├── README.md                    # This file
├── .gitignore                   # Git exclusions
│
├── notebooks/                   # Databricks notebooks
│   ├── bronze_to_s3.ipynb      # Main pipeline (parallel batch processing)
│   ├── test_s3_connection.ipynb # S3 connectivity validation
│   ├── create_securityhub_controls_reference.ipynb # Reference table generator
│   └── bronze_to_gold_v2.ipynb # Alternative pipeline
│
├── docs/                        # Documentation
│   ├── BRONZE_TO_S3.md         # Pipeline documentation (Japanese)
│   └── GOLD_STORAGE_OPTIONS.md # Storage strategy analysis
│
├── data/                        # Reference data
│   └── securityhub_controls.json # Control ID → severity mappings
│
├── scripts/                     # Utility scripts (future)
│
└── old/                         # Legacy code and documentation
    ├── bronze_to_gold.ipynb
    ├── bronze_to_silver.ipynb
    └── *.md
```

## Pipeline Components

### 1. Bronze to S3 Export (`notebooks/bronze_to_s3.ipynb`)
- **Purpose**: Transform raw Security Hub findings → compliance summaries → S3 CSV exports
- **Input**: Databricks Delta tables (bronze layer)
- **Output**: S3 CSV files (gzip compressed)
- **Frequency**: Daily batch job
- **Architecture**: Parallel batch processing with `.repartition("company_id")`

### 2. S3 Connection Test (`notebooks/test_s3_connection.ipynb`)
- **Purpose**: Validate S3 connectivity and write permissions
- **Tests**: Read/write access, compression, partitioning, parallel writes
- **Usage**: Run before deploying production job

### 3. Security Hub Controls Reference (`notebooks/create_securityhub_controls_reference.ipynb`)
- **Purpose**: Generate reference table mapping control IDs to correct severity levels
- **Output**: `data/securityhub_controls.json`

## Key Features

### Parallel Batch Processing
- **10x performance improvement**: 10 companies in ~1 min vs ~10 min sequential
- **Auto-scaling**: Handles 100+ companies with no code changes
- **Cost-efficient**: Databricks Serverless auto-scales compute resources

### S3 Export Strategy
- **Partitioning**: `company_id=xxx/date=YYYY-MM-DD/`
- **Format**: CSV with gzip compression (60-70% size reduction)
- **Parallel writes**: `.repartition("company_id")` enables simultaneous writes per company
- **Aurora MySQL compatible**: `LOAD DATA FROM S3` supports gzip natively

## Architecture

```
┌─────────────────────────────────────────┐
│  BRONZE Layer (Databricks Delta Tables) │
│  - ASFF format (AWS Security Hub)      │
│  - OCSF format (Security Lake)         │
└─────────────────────────────────────────┘
                  ↓
      [ Databricks Processing ]
                  ↓
┌─────────────────────────────────────────┐
│  Transformation (7 steps)               │
│  1. 48-hour data window extraction     │
│  2. ASFF & OCSF normalization          │
│  3. Finding deduplication              │
│  4. Severity correction                │
│  5. Control-level aggregation          │
│  6. Regional summary creation          │
│  7. Account summary creation           │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│  S3 Export (Parallel Batch)            │
│  s3://bucket/compliance/               │
│    ├── standards_summary/              │
│    │   └── company_id=xxx/             │
│    │       └── date=YYYY-MM-DD/        │
│    │           ├── part-00000.csv.gz   │
│    │           └── _SUCCESS            │
│    └── account_compliance_summary/     │
│        └── company_id=xxx/             │
│            └── date=YYYY-MM-DD/        │
│                ├── part-00000.csv.gz   │
│                └── _SUCCESS            │
└─────────────────────────────────────────┘
                  ↓
     [ Aurora MySQL / Dashboards ]
```

## Performance

| Companies | Sequential | Parallel Batch | Speedup |
|-----------|------------|----------------|---------|
| 10        | ~10 min    | ~1 min         | 10x     |
| 50        | ~50 min    | ~5 min         | 10x     |
| 100       | ~100 min   | ~10 min        | 10x     |

## Setup

### Prerequisites
- Databricks workspace with Serverless compute
- S3 bucket with write permissions
- Unity Catalog with bronze tables

### Configuration
1. Set job parameters:
   - `CATALOG_NAME`: Unity Catalog name
   - `COMPANY_INDEX_ID`: Specific company or blank for all

2. Configure S3 path in notebook:
   ```python
   s3_base_path = "s3://your-bucket/path/compliance"
   ```

### Testing
1. Run `notebooks/test_s3_connection.ipynb` to validate S3 connectivity
2. Verify all 5 tests pass before production deployment

## File Structure

### Notebooks
- **Main pipeline**: `notebooks/bronze_to_s3.ipynb`
- **Testing**: `notebooks/test_s3_connection.ipynb`
- **Reference data**: `notebooks/create_securityhub_controls_reference.ipynb`

### Documentation
- **Pipeline details**: `docs/BRONZE_TO_S3.md` (Japanese)
- **Storage analysis**: `docs/GOLD_STORAGE_OPTIONS.md`

### Data
- **Reference table**: `data/securityhub_controls.json`

## Documentation

- **[docs/BRONZE_TO_S3.md](docs/BRONZE_TO_S3.md)**: Comprehensive pipeline documentation (Japanese)
- **[docs/GOLD_STORAGE_OPTIONS.md](docs/GOLD_STORAGE_OPTIONS.md)**: Analysis of storage options (S3 vs Delta)

## Output Schema

### Standards Summary
```csv
company_id,cf_processed_time,account_id,region_id,control_pass_score,total_rules,total_passed,standards_summary,date
company_001,2024-12-26 00:00:00,123456789012,us-east-1,87.5,120,105,"[{\"std\":\"...\"}]",2024-12-26
```

### Account Compliance Summary
```csv
company_id,cf_processed_time,account_id,score,total_rules,total_passed,date
company_001,2024-12-26 00:00:00,123456789012,88.0,240,211,2024-12-26
```

## Technology Stack

- **Databricks**: Serverless compute, Delta Lake
- **PySpark**: Data transformation and aggregation
- **S3**: Output storage
- **Aurora MySQL**: Downstream data warehouse
- **Python**: Orchestration and utilities

## License

[Your License]

## Contributors

[Your Team]
