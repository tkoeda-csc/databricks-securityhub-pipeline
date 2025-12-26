# Bronze to Gold ETL Pipeline (Direct, No Silver)

## Overview

This notebook transforms AWS Security Hub findings from bronze tables directly into gold-level compliance summaries **without persisting an intermediate silver layer**. The pipeline is **AWS Security Hub CSPM-compliant**, matching Security Hub's control status calculation logic exactly.

### Architecture

**Bronze → Gold (Direct)**
- Loads ASFF/OCSF data from bronze tables
- Deduplicates findings in-memory (ASFF preferred over OCSF)
- Aggregates to gold table (account/region compliance summary)
- No silver layer (saves storage and processing time)

### Key Features

- ✅ **AWS Security Hub CSPM compliance** - Matches Security Hub control status logic
- ✅ **1-day retention** - Gold table stores only the latest state
- ✅ **Suppressed/Archived handling** - Excludes archived, conditionally excludes suppressed
- ✅ **Multi-company support** - Auto-discovers companies or processes single company
- ✅ **Error isolation** - Company failures don't stop other companies

---

## Data Sources

### Bronze Tables (Input)

**Table 1: ASFF Format**
- Path: `cloudfastener.{company_id}.aws_securityhub_findings_1_0`
- Format: AWS Security Finding Format v1.0
- Priority: **Higher** (preferred in deduplication)

**Table 2: OCSF Format**
- Path: `cloudfastener.{company_id}.aws_securitylake_sh_findings_2_0`
- Format: Open Cybersecurity Schema Framework v2.0
- Priority: **Lower** (fallback if ASFF not available)

### Gold Table (Output)

**Path**: `cloudfastener.{company_id}.aws_standard_summary`

**Schema**:
| Column | Type | Description |
|--------|------|-------------|
| `company_id` | STRING | Company identifier |
| `cf_processed_time` | TIMESTAMP | Job execution time (00:00 UTC) |
| `account_id` | STRING | AWS account ID |
| `region_id` | STRING | AWS region |
| `control_pass_score` | FLOAT | Pass rate percentage (0-100) |
| `total_rules` | INT | Total distinct controls |
| `total_passed` | INT | Controls with status PASSED |
| `standards_summary` | ARRAY\<STRUCT\> | Per-standard compliance details |

**Write Strategy**: TRUNCATE + Append (replaces all data daily)

---

## Notebook Execution Flow

The notebook is structured into 10 cells with clear separation of concerns:

### Cell 1: Configuration & Initialization
**Purpose**: Set up job parameters and display execution banner

**Configuration**:
```python
catalog_name = "cloudfastener"
company_id = ""  # "" or "ALL" = auto-discovery, or specific company ID
job_date = "2025-12-22"  # ISO format date
```

**Time Window Calculation**:
```python
# 48-hour window (captures 2.6 Security Hub check cycles)
window_end = job_date at 00:00:00 UTC
window_start = window_end - 48 hours
```

**Banner Output**:
```
========================================
Bronze → Gold ETL Configuration
========================================
Catalog:        cloudfastener
Company Mode:   Auto-discover all companies
Job Date:       2025-12-22 00:00:00 UTC
Window:         48 hours (2025-12-20 00:00 → 2025-12-22 00:00)
Retention:      1-day (TRUNCATE + Append)
========================================
```

### Cell 2-3: Helper Functions
**Purpose**: Utility functions for validation and data processing

**Functions**:
- `is_valid_company_id(schema_name)` - Validates 12-char lowercase alphanumeric format
- `discover_companies(catalog_name)` - Auto-discovers company schemas in catalog
- `table_exists(table_name)` - Checks if table exists
- `normalize_finding_id(finding_id)` - Trims and normalizes finding IDs
- `parse_iso8601_to_ts(iso_string)` - Converts ISO8601 to timestamp

### Cell 4: Transform Functions
**Purpose**: Transform bronze data to canonical schema and aggregate to gold

**Functions**:
- `transform_asff(df)` - Transforms ASFF format to canonical schema
- `transform_ocsf(df)` - Transforms OCSF format to canonical schema
- `aggregate_controls(findings_df)` - Aggregates findings to control-level status
- `aggregate_to_gold(controls_df, company_id, cf_processed_time)` - Aggregates controls to gold summary

**Key Logic**:
- Excludes ARCHIVED findings
- Maps workflow status to finding_status (New/Suppressed/Resolved)
- ASFF preferred over OCSF (preference=1 vs 0)
- Count-based aggregation (CSPM-compliant)
- 4-level hierarchy: finding → control → severity → standard → account/region

### Cell 5-6: Company Discovery
**Purpose**: Determine which companies to process

**Auto-Discovery Mode** (`company_id = "" or "ALL"`):
```python
companies_to_process = discover_companies(catalog_name)
# Returns: ["xs22xw4aw73q", "ab12cd34ef56", ...]
```

**Single Company Mode** (`company_id = "xs22xw4aw73q"`):
```python
companies_to_process = [company_id]
# Returns: ["xs22xw4aw73q"]
```

### Cell 7-8: Processing Function
**Purpose**: ETL logic for a single company

**Function Signature**:
```python
def process_company(company_id, catalog_name, window_start_ts, window_end_ts, cf_processed_time):
    """
    Returns: (success: bool, message: str)
    """
```

**Execution Steps**:
1. **Define Table Names**:
   - ASFF: `cloudfastener.{company_id}.aws_securityhub_findings_1_0`
   - OCSF: `cloudfastener.{company_id}.aws_securitylake_sh_findings_2_0`
   - Gold: `cloudfastener.{company_id}.aws_standard_summary`

2. **Check Table Existence**:
   - Skip if neither ASFF nor OCSF exists
   - Return: `(False, "No bronze tables")`

3. **Load Bronze Data**:
   - Filter by time window and product_name
   - Count rows per format
   - Skip if no data: `(False, "No data in window")`

4. **Transform & Union**:
   - Apply `transform_asff()` / `transform_ocsf()`
   - Union both DataFrames
   - Normalize finding_id

5. **Deduplicate**:
   - Window by `finding_id`
   - Order by: `finding_modified_time DESC`, `_preference DESC`, `_bronze_processed_time DESC`
   - Keep row_number = 1

6. **Aggregate**:
   - Call `aggregate_controls()` → control-level status
   - Call `aggregate_to_gold()` → account/region summary

7. **Write Gold Table**:
   ```sql
   CREATE TABLE IF NOT EXISTS {gold_tbl} (...);
   TRUNCATE TABLE {gold_tbl};
   INSERT INTO {gold_tbl} ...;
   ```

8. **Return Result**:
   - Success: `(True, "Success")`
   - Error: `(False, error_message)`

### Cell 9: Sequential Execution
**Purpose**: Loop through companies and track results

```python
for company_id in companies_to_process:
    success, message = process_company(...)

    if success:
        successful_companies.append(company_id)
    elif message in ["No bronze tables", "No data in window", "No valid findings"]:
        skipped_companies.append((company_id, message))
    else:
        failed_companies.append((company_id, message))
```

**Note**: Sequential processing (not parallel) to avoid Spark cluster resource contention.

### Cell 10: Summary Output
**Purpose**: Display formatted results and raise exception if any failures

**Output Format**:
```
============================================================
BRONZE → GOLD ETL PIPELINE SUMMARY
============================================================
Total companies:       5
✓ Successful:          3
⚠️  Skipped:            1
❌ Failed:             1
============================================================

✓ SUCCESSFUL (3):
  - xs22xw4aw73q
  - ab12cd34ef56
  - gh78ij90kl12

⚠️  SKIPPED (1):
  - mn12op34qr56: No data in window

❌ FAILED (1):
  - st78uv90wx12: Table access denied

============================================================
Bronze→Gold ETL completed successfully ✓
============================================================
```

**Error Handling**:
- If any failures: Raises exception with failure count
- Otherwise: Prints success message

---

## Processing Logic

### Step 1: Company Discovery

**Auto-Discovery Mode** (`company_id = "" or "ALL"`):
1. Lists all schemas in catalog
2. Filters schemas matching company ID format (12 lowercase alphanumeric)
3. Processes each company sequentially

**Single Company Mode** (`company_id = "xs22xw4aw73q"`):
- Validates format
- Processes only that company

### Step 2: Load Bronze Data

**Time Window**: Previous 48 hours from job execution
```python
job_date = current day at 00:00:00 UTC
window = [job_date - 48 hours, job_date)
```

**Why 48 hours?**
- Security Hub runs compliance checks every 18 hours
- Updates `UpdatedAt` timestamp on EVERY check (even if status unchanged)
- EventBridge emits findings with updated timestamps
- 48-hour window = 2.6 check cycles

**Filters**:
- ASFF: `product_name = "Security Hub"` AND `RecordState != "ARCHIVED"`
- OCSF: `metadata.product.name = "Security Hub"` AND `unmapped.RecordState != "ARCHIVED"`

### Step 3: Transform to Canonical Schema

Both ASFF and OCSF are transformed to a common schema:

**Canonical Fields**:
- `finding_id` - Unique identifier
- `finding_modified_time` - Last modified timestamp
- `account_id` - AWS account
- `region_id` - AWS region
- `standard_id` - Security standard ARN
- `control_id` - Control identifier
- `compliance_status` - PASSED / FAILED / WARNING / NOT_AVAILABLE
- `finding_status` - Active / Suppressed
- `severity` - critical / high / medium / low
- Internal: `_preference` (ASFF=1, OCSF=0), `_bronze_processed_time`

**OCSF Suppression Detection**:
```python
# OCSF format stores workflow status in unmapped field
finding_status = F.when(
    F.upper(F.col("unmapped.WorkflowState")) == "SUPPRESSED",
    "Suppressed"
).otherwise("Active")
```

### Step 4: Union and Deduplicate

**Union**: Combine ASFF + OCSF findings

**Deduplication**: Keep 1 row per `finding_id`

**Priority Order**:
1. Latest `finding_modified_time` (most recent wins)
2. Higher `_preference` (ASFF=1 > OCSF=0)
3. Latest `_bronze_processed_time` (tie-breaker)

```python
Window.partitionBy("finding_id")
      .orderBy(
          F.col("finding_modified_time").desc(),
          F.col("_preference").desc(),
          F.col("_bronze_processed_time").desc()
      )
```

Result: Deduplicated findings in-memory (no persistence)

### Step 5: Add Security Hub Compliance Fields

**Mark Suppressed Findings**:
```python
is_suppressed = F.when(
    F.upper(F.col("finding_status")) == "SUPPRESSED",
    True
).otherwise(False)
```

**Add Severity Ranking**:
```python
severity_rank = F.when(F.col("severity") == "critical", 4)
                 .when(F.col("severity") == "high", 3)
                 .when(F.col("severity") == "medium", 2)
                 .when(F.col("severity") == "low", 1)
                 .otherwise(0)
```

Purpose: Proper max aggregation (avoids lexicographic ordering: "medium" > "critical")

### Step 6: Aggregate to Gold

**4-Level Hierarchy**:

**Level 1: Control-Level**
Group by: `account_id`, `region_id`, `standard_id`, `control_id`

Count-based aggregation (excludes suppressed conditionally):
```python
active_cnt = sum(when(~is_suppressed, 1).otherwise(0))
failed_cnt = sum(when(~is_suppressed AND compliance_status == "FAILED", 1).otherwise(0))
passed_cnt = sum(when(~is_suppressed AND compliance_status == "PASSED", 1).otherwise(0))
unknown_cnt = sum(when(~is_suppressed AND compliance_status IN ("WARNING","NOT_AVAILABLE"), 1).otherwise(0))
max_severity_rank = max(severity_rank)
```

**Control Status** (AWS Security Hub precedence):
```python
control_status =
    WHEN active_cnt == 0 THEN "NO_DATA"          # All suppressed
    WHEN failed_cnt > 0 THEN "FAILED"            # At least 1 failed
    WHEN unknown_cnt > 0 THEN "UNKNOWN"          # Has warning/not_available
    WHEN passed_cnt == active_cnt THEN "PASSED"  # All active passed
    ELSE "UNKNOWN"
```

**Level 2: Severity-Level**
Group by: `account_id`, `region_id`, `standard_id`, `severity`
```python
total = count(distinct control_id)
passed = count(control_status == "PASSED")
score = (passed / total) * 100
```

**Level 3: Standard-Level**
Group by: `account_id`, `region_id`, `standard_id`
```python
total = sum(total) across severities
passed = sum(passed) across severities
score = (passed / total) * 100
controls_by_severity = array of severity-level structs
```

**Level 4: Account/Region Summary**
Group by: `account_id`, `region_id`
```python
total_rules = count(distinct (standard_id, control_id))
total_passed = count(control_status == "PASSED")
control_pass_score = (total_passed / total_rules) * 100
standards_summary = array of standard-level structs
```

### Step 7: Write to Gold

**Strategy**: TRUNCATE + Append
```sql
TRUNCATE TABLE cloudfastener.{company_id}.aws_standard_summary;
INSERT INTO cloudfastener.{company_id}.aws_standard_summary ...;
```

Result: Gold table contains only latest state for all accounts/regions

---

## AWS Security Hub CSPM Compliance

### Rules Implemented

**Rule 1: Ignore Archived Findings**
✅ Filtered at transform time: `RecordState != "ARCHIVED"`

**Rule 2: Exclude Suppressed from Control Status**
✅ Suppressed findings counted but excluded from control status calculation

**Rule 3: Control Status Precedence**
✅ NO_DATA → FAILED → UNKNOWN → PASSED

**Rule 4: NO_DATA for All-Suppressed Controls**
✅ Controls with `active_cnt == 0` get status "NO_DATA"

### Why Count-Based Aggregation?

**Problem with Min/Max**:
```python
# ❌ BAD: Min-based logic (suppression pollution)
all_passed = min(when(compliance_status == "PASSED", 1).otherwise(0))
# If ANY non-suppressed finding is not PASSED, all_passed = 0
# Includes suppressed findings → wrong result
```

**Solution: Count-Based**:
```python
# ✅ GOOD: Count active findings
active_cnt = sum(when(~is_suppressed, 1).otherwise(0))
passed_cnt = sum(when(~is_suppressed AND compliance_status == "PASSED", 1).otherwise(0))
all_passed = (passed_cnt == active_cnt)
```

### Suppressed Findings Behavior

| Scenario | Active Findings | Suppressed Findings | Control Status |
|----------|----------------|---------------------|----------------|
| All passed | 5 PASSED | 2 FAILED | **PASSED** ✅ |
| Mixed results | 3 PASSED, 2 FAILED | 1 PASSED | **FAILED** |
| All suppressed | 0 | 5 FAILED | **NO_DATA** ✅ |
| Warning present | 3 PASSED, 1 WARNING | 2 FAILED | **UNKNOWN** |

---

## Error Handling

### Per-Company Isolation

Each company processes in a try/except block:
- **Success**: Added to `successful_companies` list
- **Skip**: No tables/data → `skipped_companies` with reason
- **Failure**: Error → `failed_companies` with error message

### Summary Report

```
ETL Pipeline Summary
============================================================
Total companies: 5
Successful: 3
Skipped: 1
Failed: 1

✓ Successful: xs22xw4aw73q, ab12cd34ef56

⚠️  Skipped:
  - mn12op34qr56: No data in window

❌ Failed:
  - st78uv90wx12: Table access denied
============================================================
```

---

## Design Decisions

### Why No Silver Table?

**Removed because**:
- Gold stores only latest state (not historical)
- Can reprocess from bronze if gold fails
- Saves millions/billions of rows of storage
- Faster processing (one less I/O operation)

**Trade-off**: Can't query individual findings (only aggregated summaries)

### Why TRUNCATE Instead of MERGE?

**Changed from MERGE** (`account_id + region_id` upsert) to **TRUNCATE + Append**

**Rationale**:
- 1-day retention = no historical data to preserve
- TRUNCATE is metadata-only (instant, cheap)
- Prevents stale account/region data
- Simpler than MERGE logic

### Why ASFF Priority Over OCSF?

**ASFF is the authoritative source format**. OCSF is derived/transformed data. When both exist with same timestamp, trust ASFF.

---

## Validation Queries

**Check Latest Gold State**:
```sql
SELECT company_id, account_id, region_id, cf_processed_time,
       control_pass_score
FROM cloudfastener.{company_id}.aws_standard_summary
ORDER BY control_pass_score ASC;
**Verify NO_DATA Status for Suppressed-Only Controls**:
```sql
SELECT account_id, region_id, std.std as standard_id,
       std.controls.total, std.controls.passed,
       std.score
FROM cloudfastener.{company_id}.aws_standard_summary
LATERAL VIEW explode(standards_summary) as std
WHERE std.controls.passed < std.controls.total;
-- Should show controls with score < 100%
```

**Count Account/Region Combinations**:
```sql
SELECT COUNT(*) as gold_rows
FROM cloudfastener.{company_id}.aws_standard_summary;
-- Should equal number of distinct (account_id, region_id) in bronze
```

---

## Troubleshooting

### No Companies Found

**Symptom**: `Auto-discovery mode: Found 0 companies`

**Solutions**:
1. Check catalog name is correct
2. Verify schemas exist: `SHOW SCHEMAS IN {catalog_name}`
3. Ensure schema names are 12 lowercase alphanumeric
4. Check permissions to list schemas

### Bronze Data Not Loading

**Symptom**: `No rows found in window for {company_id}`

**Solutions**:
1. Check bronze table has data in time window
2. Verify `cf_processed_time` exists with correct timezone
3. Inspect window calculation (previous 24 hours)
4. Check product_name filter matches

### Deduplication Not Working

**Symptom**: More gold rows than expected

**Solutions**:
1. Check `finding_id` normalization (trim, nulls)
2. Verify `finding_modified_time` is parsed correctly
3. Inspect `_preference` values (ASFF=1, OCSF=0)
4. Verify Window partition key is only `finding_id`

---

## Migration from Silver-Based Pipeline

### What Changes

1. **No silver table** - Not created or updated
2. **Gold write strategy** - TRUNCATE + Append (not MERGE)
3. **No historical data** - Gold stores only 1-day snapshot
4. **CSPM compliance** - Added NO_DATA status, suppressed/archived handling

### Migration Steps

1. ✅ Run `bronze_to_gold_v2.ipynb` instead of original
2. ⚠️  **Important**: DROP existing gold table to recreate with new schema:
   ```sql
   DROP TABLE IF EXISTS cloudfastener.{company_id}.aws_standard_summary;
   ```
3. ✅ Notebook will CREATE TABLE with new schema (company_id, FLOAT types)
4. ✅ Verify controls with all suppressed findings show as "NO_DATA"
5. ✅ Delete silver tables if no longer needed

---

**Version**: 2.1 (CSPM-Compliant)
**Last Updated**: 2025-12-22
**For detailed CSPM compliance rules**: See [bronze_to_gold_v2_compliance.md](bronze_to_gold_v2_compliance.md)
**For complete schema documentation**: See [GOLD_SCHEMA.md](GOLD_SCHEMA.md)
**For testing instructions**: See [TESTING_GUIDE.md](TESTING_GUIDE.md)
