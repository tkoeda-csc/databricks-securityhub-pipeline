# Bronze to S3 Pipeline - Chronological Execution Flow

**Notebook**: `bronze_to_s3.ipynb`
**Purpose**: Process AWS Security Hub findings from bronze tables → aggregate → export to S3 as CSV
**Date**: January 5, 2026

---

## Execution Flow Overview

```
1. Setup & Configuration
   ↓
2. Define Utility Functions
   ↓
3. Load Security Hub Controls Reference Table
   ↓
4. Discover Companies to Process
   ↓
5. For Each Company:
   a. Load Bronze Data (ASFF + OCSF)
   b. Transform to Canonical Schema
   c. Deduplicate Findings
   d. Apply Reference Severity
   e. Aggregate to Control Level
   f. Aggregate to Regional Summary
   g. Aggregate to Account Summary
   h. Write to S3
   ↓
6. Report Final Results
```

---

## 1. Setup & Configuration

### Cell 1: Import Dependencies

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import traceback
from datetime import datetime
```

**Purpose**: Import PySpark and Python libraries

### Cell 2: Get Job Parameters

```python
dbutils.widgets.text("CATALOG_NAME", "")
dbutils.widgets.text("COMPANY_INDEX_ID", "")

catalog_name = dbutils.widgets.get("CATALOG_NAME").strip()
company_index_id_param = dbutils.widgets.get("COMPANY_INDEX_ID").strip()

if not catalog_name:
    raise ValueError("Missing required param: CATALOG_NAME")
```

**What Happens**: Read job parameters from Databricks widgets
**Required**: `CATALOG_NAME` (Unity Catalog name)
**Optional**: `COMPANY_INDEX_ID` (specific company or "ALL")

### Cell 3: Configure Spark

```python
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

**What Happens**: Set timezone to UTC for timestamp consistency

### Cell 4: Define Processing Window

```python
job_date = F.date_trunc("DAY", F.current_timestamp())
window_end_ts = job_date
window_start_ts = window_end_ts - F.expr("INTERVAL 48 HOURS")
```

**What Happens**: Calculate 48-hour time window
**Window**: `[today - 48 hours, today 00:00:00]`
**Why 48 hours**: Security Hub checks every 18 hours; 48 hours ensures full coverage

### Cell 5: S3 Configuration

```python
s3_base_path = "s3://dev-cf-databricks-catalog-bucket/dev/dashboard"
date_str = datetime.now().strftime('%Y-%m-%d')
is_all_companies = not company_index_id_param or company_index_id_param.upper() == "ALL"
```

**What Happens**: Define S3 output location and processing mode
**Output Path Structure**: `s3://bucket/company_id/aws/data_type/YYYY-MM-DD/`

---

## 2. Define Utility Functions

**When**: Executed at notebook start (function definitions)
**Used Later**: During company discovery and processing

### Function: `is_valid_company_id(schema_name: str) -> bool`

```python
def is_valid_company_id(schema_name: str) -> bool:
    return (
        len(schema_name) == 12 and
        schema_name.isalnum() and
        schema_name.islower()
    )
```

**Purpose**: Validate company ID format (12 chars, lowercase alphanumeric)

### Function: `table_exists(full_name: str) -> bool`

```python
def table_exists(full_name: str) -> bool:
    try:
        return spark.catalog.tableExists(full_name)
    except Exception:
        return False
```

**Purpose**: Check if table exists in catalog

### Function: `discover_companies(catalog: str) -> list`

```python
def discover_companies(catalog: str) -> list:
    spark.sql(f"USE CATALOG {catalog}")
    schemas = spark.catalog.listDatabases()

    companies = []
    for schema in schemas:
        schema_name = schema.name

        if '.' in schema_name:
            parts = schema_name.split('.')
            if parts[0] == catalog and len(parts) == 2:
                schema_name = parts[1]
            else:
                continue

        if is_valid_company_id(schema_name):
            companies.append(schema_name)

    return sorted(companies)
```

**Purpose**: Find all valid company schemas in catalog

### Function: `normalize_finding_id(col)`

```python
def normalize_finding_id(col):
    return F.when(F.length(F.trim(col)) == 0, F.lit(None)).otherwise(F.trim(col))

def parse_iso8601_to_ts(col):
    return F.to_timestamp(col)
```

**Purpose**: Field normalization helpers

### Function: `transform_asff(df)` and `transform_ocsf(df)`

```python
def transform_asff(df):
    return (
        df
        .where(F.col("RecordState") != "ARCHIVED")
        .select(
            normalize_finding_id(F.col("finding_id")).alias("finding_id"),
            parse_iso8601_to_ts(F.col("updated_at")).alias("finding_modified_time"),
            F.upper(F.col("workflow.Status")).alias("finding_status"),
            F.col("aws_account_id").cast("string").alias("account_id"),
            F.col("finding_region").cast("string").alias("region_id"),
            F.expr("compliance.AssociatedStandards[0].StandardsId").cast("string").alias("standard_id"),
            F.col("compliance.SecurityControlId").cast("string").alias("control_id"),
            F.col("compliance.Status").cast("string").alias("compliance_status"),
            F.col("severity.Label").cast("string").alias("severity"),
            F.col("cf_processed_time").alias("_bronze_processed_time"),
            F.lit(1).alias("_preference")
        )
    )
```

**Purpose**: Transform ASFF/OCSF to canonical schema
**Key Actions**: Filter ARCHIVED, normalize fields, add preference flag

### Function: `aggregate_findings_to_controls(findings_df)`

```python
def aggregate_findings_to_controls(findings_df):
    findings = (
        findings_df
        .withColumn("compliance_status", F.upper("compliance_status"))
        .withColumn("severity", F.when(F.col("severity").isNull(), "unclassified").otherwise(F.lower("severity")))
        .withColumn("is_suppressed", F.upper(F.col("finding_status")) == F.lit("SUPPRESSED"))
        .withColumn("severity_rank", F.when(F.col("severity") == "critical", 4).when(...))
    )

    control_key = ["account_id", "region_id", "standard_id", "control_id"]

    controls = (
        findings
        .groupBy(*control_key)
        .agg(
            F.sum(F.when(~F.col("is_suppressed"), 1).otherwise(0)).alias("active_cnt"),
            F.sum(F.when((~F.col("is_suppressed")) & (F.col("compliance_status") == "FAILED"), 1).otherwise(0)).alias("failed_cnt"),
            F.sum(F.when((~F.col("is_suppressed")) & (F.col("compliance_status") == "PASSED"), 1).otherwise(0)).alias("passed_cnt"),
            F.sum(F.when((~F.col("is_suppressed")) & (F.col("compliance_status").isin("WARNING", "NOT_AVAILABLE")), 1).otherwise(0)).alias("unknown_cnt"),
            F.count("*").alias("total_cnt"),
            F.max("severity_rank").alias("max_severity_rank")
        )
        .withColumn("control_status", F.when(...))
        .withColumn("severity", F.when(...))
        .drop("max_severity_rank")
    )

    return controls
```

**Purpose**: Aggregate findings to control-level metrics
**Output**: One row per (account, region, standard, control) with status

### Function: `aggregate_account_region_summary(controls_df, company_id)`

```python
def aggregate_account_region_summary(controls_df, company_id):
    # Severity-level aggregation...
    # Standard-level aggregation...
    # Regional summary...
    # Join and return final DataFrame
```

**Purpose**: Create regional standards summary with severity breakdown

---

## 3. Load Security Hub Controls Reference Table

**When**: Before processing any companies

```python
controls_ref_table = f"{catalog_name}.reference.securityhub_controls"

try:
    if table_exists(controls_ref_table):
        controls_ref_df = spark.table(controls_ref_table).select("control_id", "severity")
        ref_count = controls_ref_df.count()
    else:
        controls_ref_df = None
except Exception as e:
    controls_ref_df = None
```

**What Happens**:

1. Check if reference table exists
2. If exists: Load control_id → severity mappings
3. If not: Set to None (will use source data severity)

**Used Later**: Override finding severity with correct values during processing

---

## 4. Discover Companies to Process

**When**: Before main loop

```python
if not company_index_id_param or company_index_id_param.upper() == "ALL":
    companies_to_process = discover_companies(catalog_name)
else:
    if not is_valid_company_id(company_index_id_param):
        raise ValueError(f"Invalid company_id format")
    companies_to_process = [company_index_id_param]
```

**What Happens**:

-   **Mode 1 (ALL)**: Auto-discover all valid companies in catalog
-   **Mode 2 (Single)**: Validate and process one specific company

**Result**: List of company IDs to process

---

## 5. Pipeline Execution Loop

**When**: Main execution starts

```python
successful_companies = []
failed_companies = []
skipped_companies = []
total_stats = {"standards_rows": 0, "account_rows": 0}

for company_id in companies_to_process:
    success, message, stats = process_company(
        company_id, catalog_name, window_start_ts, window_end_ts, s3_base_path, date_str
    )

    if success:
        successful_companies.append(company_id)
        total_stats["standards_rows"] += stats.get("standards_rows", 0)
        total_stats["account_rows"] += stats.get("account_rows", 0)
    elif message in ["No bronze tables", "No data in window", "No valid findings", "No summary data"]:
        skipped_companies.append((company_id, message))
    else:
        failed_companies.append((company_id, message))
```

**What Happens**: Loop through each company and track results

---

## 6. Per-Company Processing

**Function**: `process_company(company_id, ...)`

### Step 6a: Define Table Names

```python
asff_tbl = f"{catalog_name}.{company_id}.aws_securityhub_findings_1_0"
ocsf_tbl = f"{catalog_name}.{company_id}.aws_securitylake_sh_findings_2_0"
```

**What Happens**: Construct fully-qualified bronze table names

### Step 6b: Check Table Existence

```python
asff_exists = table_exists(asff_tbl)
ocsf_exists = table_exists(ocsf_tbl)

if not asff_exists and not ocsf_exists:
    return (False, "No bronze tables", {})
```

**What Happens**: Verify at least one source exists
**Early Exit**: Skip company if no tables found

### Step 6c: Load Bronze Data

```python
sources = []

if asff_exists:
    df_asff_raw = (
        spark.table(asff_tbl)
        .where(
            (F.col("product_name") == "Security Hub") &
            (F.col("cf_processed_time") >= window_start_ts) &
            (F.col("cf_processed_time") < window_end_ts)
        )
    )
    asff_count = df_asff_raw.count()
    if asff_count > 0:
        sources.append(("ASFF", df_asff_raw))

if ocsf_exists:
    df_ocsf_raw = (
        spark.table(ocsf_tbl)
        .where(
            (F.col("metadata.product.name") == "Security Hub") &
            (F.col("cf_processed_time") >= window_start_ts) &
            (F.col("cf_processed_time") < window_end_ts)
        )
    )
    ocsf_count = df_ocsf_raw.count()
    if ocsf_count > 0:
        sources.append(("OCSF", df_ocsf_raw))

if len(sources) == 0:
    return (False, "No data in window", {})
```

**What Happens**:

1. Load ASFF table filtered by 48-hour window
2. Load OCSF table filtered by 48-hour window
3. Only keep sources with data
4. Early exit if no data found

### Step 6d: Transform to Canonical Schema

```python
canonical_dfs = []
for src, df_raw in sources:
    if src == "ASFF":
        out = transform_asff(df_raw)
    elif src == "OCSF":
        out = transform_ocsf(df_raw)
    else:
        continue

    out = out.withColumn("finding_id", normalize_finding_id(F.col("finding_id"))) \
             .where(F.col("finding_id").isNotNull())
    canonical_dfs.append(out)

if not canonical_dfs:
    return (False, "No valid findings", {})

df_union = canonical_dfs[0]
for d in canonical_dfs[1:]:
    df_union = df_union.unionByName(d, allowMissingColumns=True)
```

**What Happens**:

1. Transform each source (ASFF/OCSF) to canonical schema
2. Normalize finding_id (trim, NULL filter)
3. Union all sources into single DataFrame

### Step 6e: Deduplicate Findings

```python
w = Window.partitionBy("finding_id").orderBy(
    F.col("finding_modified_time").desc_nulls_last(),
    F.col("_preference").desc(),
    F.col("_bronze_processed_time").desc_nulls_last()
)

findings = (
    df_union
    .withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn", "_preference", "_bronze_processed_time")
)
```

**What Happens**:

1. Partition by `finding_id` (group each unique finding)
2. Order within each group by:
    - **Priority 1**: Newest update time
    - **Priority 2**: ASFF preferred over OCSF
    - **Priority 3**: Latest batch
3. Assign row number (1=best, 2=second best, ...)
4. Keep only row number 1 (the winner)
5. Drop helper columns

**Result**: One canonical record per finding_id

### Step 6f: Apply Reference Table Severity

```python
if controls_ref_df is not None:
    findings = (
        findings
        .join(
            controls_ref_df.withColumnRenamed("severity", "ref_severity"),
            on="control_id",
            how="left"
        )
        .withColumn(
            "severity",
            F.when(F.col("ref_severity").isNotNull(), F.lower(F.col("ref_severity")))
             .otherwise(F.lower(F.col("severity")))
        )
        .drop("ref_severity")
    )
```

**What Happens**:

1. Left join findings with reference table on `control_id`
2. If reference severity exists → use it
3. Otherwise → keep finding severity

### Step 6g: Aggregate to Control Level

```python
controls = aggregate_findings_to_controls(findings)
```

**What Happens**:

1. Group findings by (account, region, standard, control)
2. Calculate metrics: active_cnt, failed_cnt, passed_cnt, unknown_cnt
3. Determine control_status (PASSED/FAILED/UNKNOWN/NO_DATA)
4. Assign severity (highest from all findings)

**Result**: One row per control with aggregated status

### Step 6h: Aggregate to Regional Summary

```python
standards_summary_df = aggregate_account_region_summary(controls, company_id)
```

**What Happens**:

1. **Severity aggregation**: Count controls per severity level
2. **Standard aggregation**: Group by standard, collect severity breakdown
3. **Regional aggregation**: Count total rules, calculate overall score
4. **Join**: Combine regional metrics with standards array
5. **Add metadata**: company_id, cf_processed_time

**Result**: One row per (company, account, region) with standards_summary array

### Step 6i: Write Standards Summary to S3

```python
standards_summary_csv = standards_summary_df.withColumn(
    "standards_summary",
    F.to_json("standards_summary")
)

standards_s3_path = f"{s3_base_path}/{company_id}/aws/standards_summary/{date_str}"

(standards_summary_csv
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .option("compression", "gzip")
 .option("quote", '"')
 .option("escape", '"')
 .csv(standards_s3_path))
```

**What Happens**:

1. Convert `standards_summary` array to JSON string
2. Write single CSV file (coalesce(1))
3. Enable gzip compression
4. Configure proper CSV quoting

**Output**: `s3://bucket/company_id/aws/standards_summary/2026-01-05/part-*.csv.gz`

### Step 6j: Aggregate Account Summary

```python
account_summary = (
    standards_summary_df
    .groupBy("company_id", "account_id")
    .agg(
        F.sum("total_rules").cast("int").alias("total_rules"),
        F.sum("total_passed").cast("int").alias("total_passed"),
        F.first("cf_processed_time").alias("cf_processed_time")
    )
    .withColumn(
        "score",
        F.round(
            F.when(F.col("total_rules") > 0, (F.col("total_passed") / F.col("total_rules")) * 100)
             .otherwise(0.0),
            7
        )
    )
    .select("company_id", "cf_processed_time", "account_id", "score", "total_rules", "total_passed")
)
```

**What Happens**:

1. Group by (company, account) - sum across all regions
2. Calculate overall account compliance score
3. Round score to 7 decimal places

**Result**: One row per account with aggregated metrics

### Step 6k: Write Account Summary to S3

```python
account_s3_path = f"{s3_base_path}/{company_id}/aws/account_compliance_summary/{date_str}"

(account_summary
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .option("compression", "gzip")
 .csv(account_s3_path))
```

**What Happens**: Write account-level CSV to S3

**Output**: `s3://bucket/company_id/aws/account_compliance_summary/2026-01-05/part-*.csv.gz`

### Step 6l: Return Success

```python
return (True, "Success", {
    "standards_rows": standards_summary_count,
    "account_rows": account_count
})
```

**What Happens**: Return success status with row counts

---

## 7. Final Summary

```python
if failed_companies:
    raise Exception(f"Pipeline failed for {len(failed_companies)} company(ies).")
```

**What Happens**:

-   Print summary of successful/skipped/failed companies
-   Raise exception if any company failed unexpectedly
-   Job succeeds only if all companies processed successfully or skipped expectedly

---

## Complete Execution Timeline

```
Time 0: Notebook Start
  ├─ Import libraries
  ├─ Get job parameters
  ├─ Configure Spark (UTC timezone)
  ├─ Calculate time window (48 hours)
  └─ Define utility functions

Time 1: Load Reference Data
  └─ Load controls reference table (control_id → severity)

Time 2: Discover Companies
  └─ Query catalog for valid company schemas

Time 3: Main Loop - For Each Company:
  │
  ├─ Company 1 (e.g., abc123456789)
  │   ├─ Check table existence
  │   ├─ Load ASFF (if exists, in time window)
  │   ├─ Load OCSF (if exists, in time window)
  │   ├─ Transform ASFF → canonical
  │   ├─ Transform OCSF → canonical
  │   ├─ Union ASFF + OCSF
  │   ├─ Deduplicate by finding_id
  │   ├─ Override severity from reference table
  │   ├─ Aggregate to control level
  │   ├─ Aggregate to regional summary
  │   ├─ Write standards_summary to S3
  │   ├─ Aggregate to account level
  │   └─ Write account_summary to S3
  │
  ├─ Company 2 (e.g., def987654321)
  │   └─ (same steps...)
  │
  └─ Company N

Time 4: Final Report
  ├─ Print summary stats
  ├─ List successful companies
  ├─ List skipped companies
  ├─ List failed companies
  └─ Raise exception if any failures

End: Job Complete
```

---

## Key Chronological Insights

### Lazy Evaluation

PySpark uses lazy evaluation - transformations are not executed until an action (like `.count()` or `.write()`) is called.

**Example**: When you write:

```python
df_transformed = df.withColumn("new_col", ...)
```

Nothing happens yet! The transformation is recorded but not executed.

**Execution triggers** (actions):

-   `.count()`
-   `.show()`
-   `.write.csv()`
-   `.collect()`

### Execution Order Within Company

1. **Load & Count**: Triggers Spark to read bronze tables
2. **Transform**: Still lazy (no execution)
3. **Deduplicate**: Adds window function (still lazy)
4. **Aggregate**: Grouping recorded (still lazy)
5. **Write to S3**: **⚡ EXECUTION HAPPENS HERE** - Spark optimizes and runs entire DAG

### Parallel vs Sequential

-   **Companies**: Processed **sequentially** (one at a time)
-   **Within company**: Spark parallelizes data processing internally
-   **ASFF + OCSF loads**: Executed in sequence, then unioned

---

## Summary

The notebook executes in this exact order:

1. Setup and configuration
2. Function definitions (not executed yet)
3. Load reference table
4. Discover companies
5. Loop through companies (sequential)
6. For each company:
    - Load → Transform → Deduplicate → Aggregate → Write
7. Report results

Each company is fully processed (load through write) before moving to the next company.
