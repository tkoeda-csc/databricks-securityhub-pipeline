# Bronze to Gold Combined ETL Notebook Documentation

## Overview
This Databricks notebook implements a complete ETL pipeline that processes AWS Security Hub findings through both transformation layers in a single execution:
1. **Bronze → Silver**: Deduplication, normalization, and schema simplification
2. **Silver → Gold**: Hierarchical aggregation and compliance scoring

**Multi-Company Support**: The notebook can automatically discover and process all companies in the catalog, or process a single specified company.

## Purpose
Consolidate the entire medallion architecture into one notebook for:
- Simplified deployment and orchestration
- Atomic processing of bronze → silver → gold
- Consistent time window across both transformations
- Single job execution for complete data pipeline
- **Automatic company discovery** - no tracking table required
- **Batch processing** - process multiple companies in one job run

## Input Parameters

The notebook expects two widget parameters:

### catalog_name (optional, default: "cloudfastener")
- Specifies the Databricks catalog containing company schemas
- Can be overridden in Terraform/job configuration for different environments
- Example: `cloudfastener` (production), `cloudfastener_dev` (development)

### company_id (optional, default: empty)
- **Empty string or "ALL"**: Auto-discover and process all companies in the catalog
- **Specific company ID**: Process only that company (validates format)
- Format: Exactly 12 lowercase alphanumeric characters (e.g., `xs22xw4aw73q`)
- Used for constructing table names (e.g., `{catalog_name}.{company_id}.aws_compliance_findings`)
- **Not included as a data column** (each company has dedicated tables)

## Company Discovery

The notebook automatically identifies valid companies by:
1. Listing all schemas in the specified catalog
2. Filtering schemas matching the company ID format:
   - Exactly 12 characters
   - Lowercase letters (a-z) and numbers (0-9) only
3. Processing each discovered company sequentially

**Company ID Validation:**
```python
def is_valid_company_id(schema_name: str) -> bool:
    return (
        len(schema_name) == 12 and
        schema_name.isalnum() and
        schema_name.islower()
    )
```

## Processing Modes

### All Companies Mode (Default)
**Configuration:**
```python
catalog_name = "cloudfastener"  # default
company_id = ""                 # empty = all companies
```

**Behavior:**
- Discovers all valid company schemas in the catalog
- Processes each company sequentially through the complete ETL pipeline
- Continues processing if individual companies fail (error isolation)
- Tracks successful, skipped, and failed companies
- Provides summary report at completion

**Use Cases:**
- Daily batch jobs processing all customers
- Initial backfill of historical data
- Regular scheduled pipeline runs

### Single Company Mode
**Configuration:**
```python
catalog_name = "cloudfastener"
company_id = "xs22xw4aw73q"    # specific company
```

**Behavior:**
- Validates company ID format (12 lowercase alphanumeric)
- Processes only the specified company
- Fails fast if company ID is invalid
- Useful for ad-hoc runs or debugging

**Use Cases:**
- Reprocessing specific customer data
- Testing with single company
- Troubleshooting data issues

## Error Handling

### Per-Company Error Isolation
Each company is processed independently:
- **Success**: Company completes bronze → silver → gold successfully
- **Skipped**: Company has no data to process (logged, not counted as failure)
  - No bronze tables exist
  - No data in processing window
  - No valid findings after filtering
  - No silver data for aggregation
- **Failed**: Company encounters an error (logged, continues to next company)
  - Error message captured and displayed in summary
  - Stack trace logged for debugging

### Summary Reporting
At job completion, the notebook displays:
- Total companies processed
- Count of successful completions
- Count of skipped companies (with reasons)
- Count of failed companies (with error messages)
- List of each category

**Example Output:**
```
===========================================================
ETL Pipeline Summary
===========================================================
Total companies: 15
Successful: 12
Skipped: 2
Failed: 1

✓ Successful: xs22xw4aw73q, rrb189wgiiez, qa2fyoogfwzn, ...

⚠️  Skipped:
  - company001: No data in window
  - company002: No bronze tables

❌ Failed:
  - company003: Table not found: cloudfastener.company003.aws_standard_summary...
===========================================================
```

## Processing Window

**Automatic Window Calculation:**
- Job calculates current date at 00:00:00 UTC (`job_date`)
- Processes data from `[job_date - 1 day, job_date]`
- Example: Job runs on 2025-12-23, processes 2025-12-22 00:00:00 to 2025-12-23 00:00:00

## Data Sources

### Bronze Tables (Input)
1. **ASFF**: `cloudfastener.{company_id}.aws_securityhub_findings_1_0`
   - AWS Security Finding Format (original)
   - **Higher priority** in deduplication (preference = 1)

2. **OCSF**: `cloudfastener.{company_id}.aws_securitylake_sh_findings_2_0`
   - Open Cybersecurity Schema Framework (normalized)
   - **Lower priority** in deduplication (preference = 0)

### Silver Table (Intermediate)
**Table**: `cloudfastener.{company_id}.aws_compliance_findings`

**Schema:**
| Column Name           | Data Type | Description                                      |
| --------------------- | --------- | ------------------------------------------------ |
| finding_id            | STRING    | Unique finding identifier                        |
| cf_processed_time     | TIMESTAMP | Job execution time (00:00:00 UTC)                |
| finding_modified_time | TIMESTAMP | Finding last modified time (UTC)                 |
| finding_status        | STRING    | Finding status (New, In Progress, etc.)          |
| account_id            | STRING    | AWS account ID                                   |
| region_id             | STRING    | AWS region                                       |
| standard_id           | STRING    | Compliance standard ID                           |
| control_id            | STRING    | Security control ID                              |
| severity              | STRING    | Finding severity level                           |
| compliance_status     | STRING    | PASSED / FAILED / WARNING / NOT_AVAILABLE        |

**Key Changes from Original Silver:**
- ❌ Dropped: `company_id` (implicit in table path, one table per company)
- ❌ Dropped: `finding_source`, `finding_created_time`, `control_title`, `rule_severity`, `resource_id`, `resource_type`, `created_at`, `updated_at`
- ✅ Changed: Use `cf_processed_time` consistently (removed `cf_processed_at` and `snapshot_at`)
- ✅ Simplified: Only essential compliance finding fields retained

### Gold Table (Output)
**Table**: `cloudfastener.{company_id}.aws_standard_summary`

**Schema:**
| Column Name       | Data Type     | Description                                      |
| ----------------- | ------------- | ------------------------------------------------ |
| cf_processed_time | TIMESTAMP     | Job execution time (00:00:00 UTC)                |
| account_id        | STRING        | AWS account ID                                   |
| region_id         | STRING        | AWS region                                       |
| control_pass_rate | FLOAT         | Overall pass rate: `total_passed / total_rules`  |
| total_rules       | INTEGER       | Total distinct controls evaluated                |
| total_passed      | INTEGER       | Number of controls that passed                   |
| standards_summary | ARRAY<STRUCT> | Per-standard compliance details                  |

**Key Changes from Original Gold:**
- ❌ Dropped: `company_id` (implicit in table path, one table per company)
- ❌ Dropped: `finding_source`, `snapshot_at`, `total_failed`, `total_unknown`
- ✅ Renamed: `gold_aws_compliance_summary` → `aws_standard_summary`
- ✅ Changed: `compliance_score` (0-100) → `control_pass_rate` (0.0-1.0)
- ✅ Changed: Use `cf_processed_time` consistently
- ✅ Simplified: `standards_summary` stores only `total` and `passed`

## Processing Logic

### Cell 1: Configuration
```python
Inputs:  catalog_name (default: "cloudfastener"), company_id (optional)
Actions:
  - Get widget parameters
  - Validate catalog_name
  - Calculate job_date = current day 00:00 UTC
  - Calculate window = [job_date - 1 day, job_date]
  - Define cf_processed_time for data records
  - Set UTC timezone
Output:  Configuration variables
```

### Cell 2: Helper Functions
- `is_valid_company_id()`: Validate company ID format (12 chars, lowercase alphanumeric)
- `discover_companies()`: List all valid company schemas in catalog
- `table_exists()`: Check table existence
- `normalize_finding_id()`: Trim and NULL-ify empty IDs
- `parse_iso8601_to_ts()`: Parse ISO timestamp strings

### Cell 3: Discover Companies
```python
Inputs:  company_id parameter, catalog_name
Actions:
  - If empty/"ALL": Discover all companies in catalog
  - If specific: Validate format and use single company
  - Filter schemas matching 12-char lowercase alphanumeric pattern
  - Print list of companies to process
Output:  companies_to_process list
```

### Cell 4: Multi-Company Processing Loop

For each company in `companies_to_process`:

#### 4.1: Bronze → Silver Pipeline

**Table Name Construction:**
```python
asff_tbl   = f"{catalog_name}.{company_id}.aws_securityhub_findings_1_0"
ocsf_tbl   = f"{catalog_name}.{company_id}.aws_securitylake_sh_findings_2_0"
silver_tbl = f"{catalog_name}.{company_id}.aws_compliance_findings"
gold_tbl   = f"{catalog_name}.{company_id}.aws_standard_summary"
```

**Bronze Data Loading:**
```python
Inputs:  ASFF and OCSF bronze tables (company-specific)
Filter:
  - product_name = "Security Hub"
  - cf_processed_time in [window_start, window_end)
Actions:
  - Check table existence
  - Count rows from each source
  - Skip company if no tables exist
  - Skip company if no data in window
```

**Transformation Functions:**

*ASFF Transformation:*
- Map `workflow.Status` to normalized statuses (New, In Progress, Suppressed, Resolved)
- Extract from `compliance.AssociatedStandards[0].StandardsId`
- Extract from `compliance.SecurityControlId`
- Set `_preference = 1` (higher priority)

*OCSF Transformation:*
- Direct field mappings (already normalized)
- Extract from `finding_info.uid`, `compliance.standards[0]`, etc.
- Set `_preference = 0` (lower priority)

**Common Output Schema:**
```
finding_id, cf_processed_time, finding_modified_time,
finding_status, account_id, region_id, standard_id, control_id,
compliance_status, severity, _preference, _bronze_processed_time
```

**Union and Deduplication:**
```python
Actions:
  1. Transform each source (ASFF/OCSF)
  2. Filter NULL finding_ids
  3. Union all DataFrames
  4. Deduplicate by finding_id:
     - Order: finding_modified_time DESC
             → _preference DESC (ASFF=1 > OCSF=0)
             → _bronze_processed_time DESC
  5. Keep row_number = 1
  6. Skip company if no valid findings
Output: Deduplicated findings per company
```

**Deduplication Logic:**
1. **Prefer most recent**: Newest `finding_modified_time` wins
2. **Then prefer ASFF**: If same timestamp, internal `_preference` (ASFF=1 > OCSF=0) determines winner
3. **Then prefer latest processing**: Latest bronze `_bronze_processed_time` as tie-breaker

**Note:** The `_preference` and `_bronze_processed_time` fields are used only during deduplication and are **not persisted** to the silver table.

**Merge to Silver:**
```python
Match Key: finding_id

UPDATE when:
  - target.finding_modified_time IS NULL, OR
  - source.finding_modified_time > target.finding_modified_time

INSERT when:
  - No matching finding_id exists

Result: Upserted silver table
```

### Stage 2: Silver → Gold

#### Cell 8: Load Silver for Gold Processing
```python
Inputs:  Silver table (company-specific)
Filter:
  - cf_processed_time = job_date
Actions:
  - Normalize compliance_status to UPPERCASE
  - Replace NULL severity with "unclassified"
Exit:    If no silver data → "NO_SILVER_DATA"
```

#### Cell 9: Control-Level Aggregation
```python
Group By: (cf_processed_time, account_id, region_id,
          standard_id, control_id)

Aggregations:
  - has_failed   = 1 if any finding FAILED
  - all_passed   = 1 if all findings PASSED
  - has_unknown  = 1 if any WARNING or NOT_AVAILABLE
  - severity     = MAX(severity) across findings

Control Status:
  IF has_failed = 1        → FAILED
  ELSE IF all_passed = 1   → PASSED
  ELSE                     → UNKNOWN
```

**Logic:**
- A control fails if ANY finding for that control fails
- A control passes only if ALL findings pass
- Otherwise, control status is unknown

#### Cell 10: Severity-Level Aggregation
```python
Group By: (cf_processed_time, account_id, region_id,
          standard_id, severity)

Aggregations:
  - total  = DISTINCT COUNT(control_id)
  - passed = COUNT(control_status = PASSED)
  - score  = (passed / total) * 100

Note: Failed and unknown counts calculated internally but not stored
```

#### Cell 11: Standard-Level Aggregation
```python
Group By: (cf_processed_time, account_id, region_id,
          standard_id)

Aggregations:
  - total  = SUM(total) across all severities
  - passed = SUM(passed) across all severities
  - score  = (passed / total) * 100

Structure:
  standard_summary: {
    std: <standard_id>,
    score: <float>,
    controls: {
      total: <int>,
      passed: <int>
    },
    controls_by_severity: [
      {
        level: <severity>,
        score: <float>,
        controls: {
          total: <int>,
          passed: <int>
        }
      },
      ...
    ]
  }
```

#### Cell 12: Account/Region Summary
```python
Group By: (cf_processed_time, account_id, region_id)

Aggregations:
  - total_rules  = DISTINCT COUNT((standard_id, control_id))
  - total_passed = SUM(control_status = PASSED)
  - control_pass_rate = total_passed / total_rules (as float 0.0-1.0)

Join: Collect standards_summary array from cell 11
```

#### Cell 13: Merge to Gold
```python
Match Key: (cf_processed_time, account_id, region_id)

UPDATE when matched: Set all columns
INSERT when not matched: Insert new row

Result: Gold table updated
```

## Data Flow Diagram

```
ASFF Bronze Table     ──┐
(24-hour window)        │
                        ├──> Transform to silver schema
OCSF Bronze Table     ──┘    (normalize statuses, extract fields)
(24-hour window)
                        ↓
                     Union DataFrames
                        ↓
                  Deduplicate by finding_id
              (prefer: recent → ASFF → latest)
                        ↓
              MERGE into Silver Table ───────┐
              (upsert by finding_id)         │
                                             │
                                             ↓
                                  Load silver (job_date)
                                             ↓
                              Control-Level Aggregation
                             (determine PASSED/FAILED)
                                             ↓
                             Severity-Level Aggregation
                            (group by standard+severity)
                                             ↓
                            Standard-Level Aggregation
                          (rollup with nested structure)
                                             ↓
                           Account/Region Aggregation
                            (overall compliance score)
                                             ↓
                              MERGE into Gold Table
                          (upsert by account+region)
```

## Key Features

### 1. Unified Processing
- Single notebook execution processes entire pipeline
- Atomic transaction ensures consistency
- Shared time window across transformations
- Eliminates orchestration complexity between jobs

### 2. Deduplication Strategy (ASFF Priority)
**Changed from Original:**
- ✅ **ASFF now has higher priority** (internal `_preference` = 1)
- ❌ Original prioritized OCSF
- ✅ **Partition by finding_id only** (no company_id needed)

**Rationale:**
- ASFF is the authoritative source format
- OCSF is derived/transformed data
- When both exist with same timestamp, trust ASFF
- Each company has dedicated tables, so company_id not needed in partition
- `_preference` used only during deduplication, not persisted to silver

**Tie-Breaking Order:**
1. Most recent `finding_modified_time`
2. Higher internal `_preference` (ASFF=1 > OCSF=0)
3. Most recent `_bronze_processed_time` from bronze table

### 3. Simplified Silver Schema
**Removed Columns:**
- `company_id`: Implicit in table path (each company has dedicated tables)
- `source_preference`: Used only during deduplication, not persisted (internal `_preference` field)
- `finding_source`: No longer tracked
- `finding_created_time`: Only modified time matters
- `control_title`, `rule_severity`: Not needed for aggregation
- `resource_id`, `resource_type`: Resource-level tracking removed
- `created_at`, `updated_at`: Replaced by `cf_processed_time`

**Benefits:**
- Smaller table size
- Faster queries (no redundant filtering by company_id)
- Simpler maintenance
- Focus on essential compliance data
- Natural partitioning at table level vs. column level

### 4. Gold Aggregation Changes

**Metric Change:**
- Old: `compliance_score` = percentage (0-100)
- New: `control_pass_rate` = ratio (0.0-1.0)

**Structure Change:**
- Old: Stored `total`, `passed`, `failed`, `unknown`
- New: Stores only `total` and `passed`
- Failed/unknown calculated as `total - passed` if needed

**Rationale:**
- Pass rate is the primary business metric
- Reduces data redundancy
- Failed count = total - passed (derivable)

### 5. Automatic Window Management
- No manual `snapshot_at` parameter needed
- Job automatically determines processing date
- Consistent with daily scheduling patterns
- Window always covers previous full day

### 6. Control Status Logic
```
Control has multiple findings (resources):
  - ANY finding FAILED        → Control = FAILED
  - ALL findings PASSED       → Control = PASSED
  - Mixed or WARNING/N/A      → Control = UNKNOWN
```

This ensures:
- Failures are never hidden by passing resources
- Only fully compliant controls marked as PASSED
- Ambiguous states properly tracked

### 7. Score Calculation
All scores use consistent formula:
```
score = (passed / total) * 100
```

Applied at three levels:
- **Severity level**: Within standard+severity combination
- **Standard level**: Across all severities in standard
- **Account level**: As `control_pass_rate` (divided by 100 → 0.0-1.0)

### 8. Graceful Exits
- **EMPTY_WINDOW**: No bronze data in time window
- **EMPTY_CANONICAL**: No valid findings after filtering
- **NO_SILVER_DATA**: No silver data for gold processing

Each exit point provides clear reason for early termination.

## Execution Flow Summary

1. **Setup** (Cells 1-3)
   - Get parameters, calculate window, define helpers

2. **Bronze → Silver** (Cells 4-7)
   - Load: Read bronze tables in window
   - Transform: Normalize ASFF/OCSF to silver schema
   - Deduplicate: Prefer recent, then ASFF, then latest
   - Merge: Upsert into silver table

3. **Silver → Gold** (Cells 8-13)
   - Load: Read silver for current job_date
   - Control: Determine control-level status
   - Severity: Aggregate by severity within standards
   - Standard: Rollup with nested structure
   - Account: Calculate overall compliance metrics
   - Merge: Upsert into gold table

4. **Complete**
   - Log final status
   - Exit successfully

## Use Cases

### Single-Job Deployment
- Deploy one notebook instead of two
- Simplify Databricks job configuration
- Reduce orchestration complexity

### Daily Batch Processing
- Schedule job once per day
- Automatically processes previous day's data
- No manual date parameter management

### Data Consistency
- Atomic processing ensures silver and gold are in sync
- Same time window across both layers
- Eliminates temporal skew between layers

### Development and Testing
- Easier to test entire pipeline
- Single notebook to modify/debug
- Clear cell-by-cell execution flow

## Performance Considerations

### Bronze Stage
- Filter early on `cf_processed_time` window
- Filter early on `product_name`
- Union only non-empty sources

### Silver Stage
- Filter NULL finding_ids before union (reduces data size)
- Window function partitioned by (company_id, finding_id) only
- Deduplicate before merge (reduces merge overhead)

### Gold Stage
- Load only current job_date from silver
- Progressive aggregation (control → severity → standard → account)
- Collect_list only at standard level (manageable array size)

### Optimization Tips
- Enable adaptive query execution
- Partition silver table by `cf_processed_at`
- Partition gold table by `cf_processed_at` and `account_id`
- Use appropriate cluster size for data volume

## Troubleshooting

### No Silver Updates
- Check bronze tables exist and have data
- Verify `cf_processed_time` in correct window
- Confirm `product_name = "Security Hub"` filter matches
- Check finding_ids are not NULL

### Gold Aggregation Issues
- Verify silver data exists for job_date
- Check `compliance_status` values are valid
- Ensure control_ids are not NULL
- Verify severity values are normalized

### Deduplication Not Working
- Check `finding_modified_time` is properly parsed
- Verify preference values (ASFF=1, OCSF=0)
- Confirm window function partition keys match

### Performance Issues
- Reduce time window if processing too much data
- Check for data skew in account_id or region_id
- Consider partitioning tables
- Review cluster size and autoscaling settings

## Maintenance

### Adding New Columns
1. Update transformation functions (Cell 5)
2. Update merge statement (Cell 7)
3. Update silver schema in documentation

### Changing Aggregation Logic
1. Modify control-level logic (Cell 9)
2. Update downstream aggregations if needed
3. Test with historical data

### Supporting New Standards
- No code changes required
- Pipeline automatically handles new standard_ids
- Gold structure adapts dynamically

### Adding New Companies
- **No code changes required**
- New companies automatically discovered when schemas are created
- Simply create new schema with format: `{catalog_name}.{12_char_company_id}`
- Next job run will include the new company

## Assumptions & Dependencies

- Bronze tables use consistent schema
- `cf_processed_time` exists in bronze tables
- `product_name` or `metadata.product.name` = "Security Hub"
- Silver and gold tables pre-exist with matching schemas for each company
- Finding IDs are globally unique within a company's tables
- Databricks `dbutils` available for widgets
- UTC timezone for all timestamps
- Daily job scheduling recommended (once per day)
- **Company ID format**: Exactly 12 lowercase alphanumeric characters
- **Catalog contains only company schemas** (or non-matching schemas will be filtered out)
