# S3 Output Structure

## Base Path

```
s3://dev-cf-databricks-catalog-bucket/dev/dashboard/
```

## Directory Tree

### Current Implementation (bronze_to_s3.ipynb - CSV)

```
s3://dev-cf-databricks-catalog-bucket/dev/dashboard/
├── {company_id}/                          # 12-char lowercase alphanumeric
│   └── aws/
│       ├── standards_summary/
│       │   └── {YYYY-MM-DD}.csv.gz        # Regional compliance by standard
│       │       └── part-00000-*.csv.gz    # Spark-generated filename
│       │
│       └── account_compliance_summary/
│           └── {YYYY-MM-DD}.csv.gz        # Account-level compliance scores
│               └── part-00000-*.csv.gz
```

### JSONL Implementation (bronze_to_s3_jsonl.ipynb)

```
s3://dev-cf-databricks-catalog-bucket/dev/dashboard/
├── {company_id}/
│   └── aws/
│       ├── standards_summary/
│       │   └── {YYYY-MM-DD}/              # Date as folder (not file)
│       │       └── part-00000-*.json.gz   # JSONL format
│       │
│       └── account_compliance_summary/
│           └── {YYYY-MM-DD}/
│               └── part-00000-*.json.gz
```

## Key Differences

| Aspect       | CSV (bronze_to_s3.ipynb)          | JSONL (bronze_to_s3_jsonl.ipynb) |
| ------------ | --------------------------------- | -------------------------------- |
| Date in path | Filename: `2026-01-05.csv.gz`     | Folder: `2026-01-05/`            |
| File naming  | `2026-01-05.csv.gz/part-*.csv.gz` | `2026-01-05/part-*.json.gz`      |
| Format       | CSV with headers                  | JSONL (one JSON per line)        |
| Compression  | gzip                              | gzip                             |

## Example Paths

### CSV Format

```
s3://dev-cf-databricks-catalog-bucket/dev/dashboard/abc123456789/aws/standards_summary/2026-01-05.csv.gz/part-00000-xxx.csv.gz
s3://dev-cf-databricks-catalog-bucket/dev/dashboard/abc123456789/aws/account_compliance_summary/2026-01-05.csv.gz/part-00000-xxx.csv.gz
```

### JSONL Format

```
s3://dev-cf-databricks-catalog-bucket/dev/dashboard/abc123456789/aws/standards_summary/2026-01-05/part-00000-xxx.json.gz
s3://dev-cf-databricks-catalog-bucket/dev/dashboard/abc123456789/aws/account_compliance_summary/2026-01-05/part-00000-xxx.json.gz
```

## Multi-Company Structure

```
s3://dev-cf-databricks-catalog-bucket/dev/dashboard/
├── abc123456789/          # Company 1
│   └── aws/
│       ├── standards_summary/
│       │   ├── 2026-01-04.csv.gz
│       │   └── 2026-01-05.csv.gz
│       └── account_compliance_summary/
│           ├── 2026-01-04.csv.gz
│           └── 2026-01-05.csv.gz
│
├── def987654321/          # Company 2
│   └── aws/
│       ├── standards_summary/
│       │   └── 2026-01-05.csv.gz
│       └── account_compliance_summary/
│           └── 2026-01-05.csv.gz
│
└── xyz111222333/          # Company 3
    └── aws/
        └── ...
```

## Data Flow

```
Databricks (Spark)
    ↓
Transform & Aggregate
    ↓
Write to S3
    ├── standards_summary      (regional, by standard, with severity breakdown)
    └── account_compliance     (account-level aggregation)
    ↓
AWS Lambda (triggered by S3)
    ↓
Parse files (CSV or JSONL)
    ↓
Insert into Aurora PostgreSQL
```

## File Characteristics

### Standards Summary

-   **Granularity**: Account + Region + Standard
-   **Includes**:
    -   Overall compliance score
    -   Control counts (total/passed)
    -   Per-severity breakdown
    -   Standards details array

### Account Compliance Summary

-   **Granularity**: Account-level (aggregated from regional)
-   **Includes**:
    -   Total rules across all regions
    -   Total passed
    -   Overall compliance score (percentage)

## Note on Partitioning

Current implementation uses **date in filename/folder** but NOT Spark partitioning.

-   CSV: `.csv(path)` writes to single location with date in path
-   JSONL: `.json(path)` writes to single location with date in path
-   Both use `coalesce(1)` to produce single file per company per date
