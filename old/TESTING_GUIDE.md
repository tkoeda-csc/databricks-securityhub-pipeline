# Testing Guide: bronze_to_gold_v2.ipynb in Databricks

## Pre-Flight Checklist

### 1. Verify Bronze Data Exists

```sql
-- Check ASFF table
SELECT COUNT(*) as row_count,
       MIN(cf_processed_time) as oldest,
       MAX(cf_processed_time) as newest
FROM cloudfastener.{company_id}.aws_securityhub_findings_1_0
WHERE product_name = 'Security Hub';

-- Check OCSF table
SELECT COUNT(*) as row_count,
       MIN(cf_processed_time) as oldest,
       MAX(cf_processed_time) as newest
FROM cloudfastener.{company_id}.aws_securitylake_sh_findings_2_0
WHERE metadata.product.name = 'Security Hub';
```

**Expected:** At least some rows with recent `cf_processed_time` (within last 24 hours)

---

### 2. Verify Gold Table Exists

```sql
-- Check if gold table exists
DESCRIBE TABLE cloudfastener.{company_id}.aws_standard_summary;
```

**If table doesn't exist, create it:**
```sql
CREATE TABLE cloudfastener.{company_id}.aws_standard_summary (
  company_id STRING,
  cf_processed_time TIMESTAMP,
  account_id STRING,
  region_id STRING,
  control_pass_score FLOAT,
  total_rules INT,
  total_passed INT,
  standards_summary ARRAY<STRUCT<
    std: STRING,
    score: FLOAT,
    controls: STRUCT<total: INT, passed: INT>,
    controls_by_severity: ARRAY<STRUCT<
      level: STRING,
      score: FLOAT,
      controls: STRUCT<total: INT, passed: INT>
    >>
  >>
)
USING DELTA
LOCATION 's3://your-bucket/gold/{company_id}/aws_standard_summary/';
```

---

## Testing Strategy

### Phase 1: Single Company Test (Recommended)

#### Step 1: Upload Notebook
1. Open Databricks workspace
2. Go to **Workspace → Users → [Your User]**
3. Click **Import**
4. Upload `bronze_to_gold_v2.ipynb`

#### Step 2: Configure Parameters
In the notebook, edit Cell 1 or use widgets:
```python
catalog_name = "cloudfastener"
company_id = "xs22xw4aw73q"  # Pick ONE company for testing
```

#### Step 3: Run Cell-by-Cell
**Cell 1 (Configuration):**
```python
# ✅ Expected output:
# catalog_name = cloudfastener
# job_date = 2025-12-22 00:00:00
# window_start = window_end - 1 day
```

**Cell 3 (Helper Functions):**
```python
# ✅ Should complete without errors
# No output expected
```

**Cell 5 (Company Discovery):**
```python
# ✅ Expected output:
# Single company mode: xs22xw4aw73q
# Total companies to process: 1
```

**Cell 7 (Main Processing Loop):**
This is the critical cell. Watch for:

**Expected Output:**
```
============================================================
Processing company: xs22xw4aw73q
============================================================
ASFF bronze      = cloudfastener.xs22xw4aw73q.aws_securityhub_findings_1_0
OCSF bronze      = cloudfastener.xs22xw4aw73q.aws_securitylake_sh_findings_2_0
Gold             = cloudfastener.xs22xw4aw73q.aws_standard_summary
------------------------------------------------------------
ASFF exists? True
OCSF exists? True
ASFF rows in window: 15234
OCSF rows in window: 12891
Union rows: 28125
Deduplicated findings: 15234
Active findings (non-suppressed): 14821
Suppressed findings: 413
Gold summary rows: 12
✓ Gold table overwritten (1-day retention)
✓ Successfully processed xs22xw4aw73q

============================================================
ETL Pipeline Summary
============================================================
Total companies: 1
Successful: 1
Skipped: 0
Failed: 0

✓ Successful: xs22xw4aw73q
============================================================
```

**Key Metrics to Check:**
1. ✅ Union rows ≈ sum of ASFF + OCSF rows
2. ✅ Deduplicated findings < Union rows (duplicates removed)
3. ✅ Active findings + Suppressed findings = Deduplicated findings
4. ✅ Gold summary rows > 0 (at least 1 account/region)

---

### Phase 2: Validation Queries

#### Query 1: Check Gold Table Contents

```sql
SELECT
    cf_processed_time,
    account_id,
    region_id,
    control_pass_score,
    total_rules,
    total_passed,
    SIZE(standards_summary) as num_standards
FROM cloudfastener.xs22xw4aw73q.aws_standard_summary
ORDER BY control_pass_score ASC;
```

**Expected:**
- `cf_processed_time` = today at 00:00:00 UTC
- Multiple rows (one per account/region combination)
- `control_pass_score` between 0.0 and 100.0
- `total_passed <= total_rules`
- `num_standards >= 1`

---

#### Query 2: Inspect Standards Detail

```sql
SELECT
    account_id,
    region_id,
    explode(standards_summary) as std
FROM cloudfastener.xs22xw4aw73q.aws_standard_summary;
```

**Expected:**
- `std.std` = standard ARN (e.g., `arn:aws:securityhub:...`)
- `std.score` between 0.0 and 100.0
- `std.controls.total >= 1`
- `std.controls.passed <= std.controls.total`

---

#### Query 3: Check for NO_DATA Controls

**Build control-level view:**
```sql
CREATE OR REPLACE TEMPORARY VIEW control_details AS
SELECT
    account_id,
    region_id,
    std.std as standard_id,
    -- Need to reverse-engineer control status from findings
    -- (Our aggregation happens in-memory, so we can't query controls directly)
    'See next query' as note
FROM cloudfastener.xs22xw4aw73q.aws_standard_summary
LATERAL VIEW explode(standards_summary) as std;
```

**Note:** Our pipeline doesn't persist control-level data, only standard-level summaries. To validate NO_DATA controls, you need to:

**Option A: Add Debug Output to Notebook**

Add this to Cell 7 after control aggregation (line ~340):
```python
# DEBUG: Show controls with NO_DATA status
no_data_controls = controls.where(F.col("control_status") == "NO_DATA")
no_data_count = no_data_controls.count()
print(f"Controls with NO_DATA status: {no_data_count}")

if no_data_count > 0:
    print("Sample NO_DATA controls:")
    no_data_controls.select("account_id", "region_id", "standard_id", "control_id", "active_cnt", "total_cnt").show(10, truncate=False)
```

---

#### Query 4: Compare to Security Hub Console

**Gold Table Query:**
```sql
SELECT
    account_id,
    region_id,
    SUM(std.controls.total) as total_controls,
    SUM(std.controls.passed) as passed_controls,
    ROUND(SUM(std.controls.passed) * 100.0 / SUM(std.controls.total), 2) as overall_score
FROM (
    SELECT
        account_id,
        region_id,
        explode(standards_summary) as std
    FROM cloudfastener.xs22xw4aw73q.aws_standard_summary
)
GROUP BY account_id, region_id;
```

**Security Hub Console:**
1. Open AWS Security Hub console
2. Navigate to **Standards**
3. Select same account/region
4. Compare scores

**Expected:** Scores should be **close** but may not match exactly due to:
- Timing differences (gold = last 24h, console = real-time)
- Control inventory differences (gold = observed findings only)

---

### Phase 3: CSPM Compliance Validation

#### Test Case 1: Verify Suppressed Findings are Excluded

**Add debug output to Cell 7 (line ~325):**
```python
# DEBUG: Check control with suppressed findings
debug_control = controls.where(
    (F.col("control_id") == "S3.8") &  # Pick a control you know has suppressed findings
    (F.col("active_cnt") < F.col("total_cnt"))
)
if debug_control.count() > 0:
    print("Control with suppressed findings:")
    debug_control.select("control_id", "active_cnt", "total_cnt", "control_status").show(truncate=False)
```

**Expected:** If `active_cnt < total_cnt`, some findings are suppressed, and `control_status` should reflect only non-suppressed findings.

---

#### Test Case 2: Verify NO_DATA Status for All-Suppressed Controls

**Query bronze for all-suppressed controls:**
```sql
-- Find findings for a specific control
SELECT
    compliance.SecurityControlId as control_id,
    workflow.Status as workflow_status,
    RecordState as record_state,
    COUNT(*) as finding_count
FROM cloudfastener.xs22xw4aw73q.aws_securityhub_findings_1_0
WHERE
    compliance.SecurityControlId = 'EC2.1'  -- Pick a control
    AND product_name = 'Security Hub'
    AND cf_processed_time >= current_timestamp() - INTERVAL 1 DAY
GROUP BY compliance.SecurityControlId, workflow.Status, RecordState;
```

**If all findings have `workflow_status = "SUPPRESSED"`:**

**Add debug to Cell 7:**
```python
# DEBUG: Find NO_DATA controls
no_data = controls.where(F.col("control_status") == "NO_DATA")
print(f"NO_DATA controls count: {no_data.count()}")
no_data.select("control_id", "active_cnt", "total_cnt", "control_status").show(20, truncate=False)
```

**Expected:** Control appears with `control_status = "NO_DATA"` ✅

---

#### Test Case 3: Verify Severity Aggregation

**Add debug to Cell 7 (line ~340):**
```python
# DEBUG: Check severity aggregation
severity_test = controls.where(F.col("control_id") == "IAM.1")  # Pick a control
severity_test.select("control_id", "severity", "max_severity_rank").show(truncate=False)
```

**Then query bronze to see raw severities:**
```sql
SELECT
    compliance.SecurityControlId as control_id,
    severity.Label as severity,
    COUNT(*) as count
FROM cloudfastener.xs22xw4aw73q.aws_securityhub_findings_1_0
WHERE
    compliance.SecurityControlId = 'IAM.1'
    AND product_name = 'Security Hub'
    AND cf_processed_time >= current_timestamp() - INTERVAL 1 DAY
    AND RecordState != 'ARCHIVED'
GROUP BY compliance.SecurityControlId, severity.Label;
```

**Expected:** If raw severities include ["CRITICAL", "LOW", "MEDIUM"], gold should show `severity = "critical"` ✅

---

### Phase 4: Multi-Company Test

#### Step 1: Change Parameters

```python
catalog_name = "cloudfastener"
company_id = ""  # or "ALL"
```

#### Step 2: Run Full Pipeline

**Watch for:**
- Per-company progress logs
- Success/Skip/Fail counts
- Error messages for failed companies

**Expected Output:**
```
============================================================
Processing company: xs22xw4aw73q
============================================================
... (processing logs) ...
✓ Successfully processed xs22xw4aw73q

============================================================
Processing company: ab12cd34ef56
============================================================
... (processing logs) ...
✓ Successfully processed ab12cd34ef56

============================================================
ETL Pipeline Summary
============================================================
Total companies: 5
Successful: 4
Skipped: 1
Failed: 0

✓ Successful: xs22xw4aw73q, ab12cd34ef56, zy98xw76vu54, mn12op34qr56

⚠️  Skipped:
  - st78uv90wx12: No data in window
============================================================
```

---

## Troubleshooting

### Error: "Table not found"

**Cause:** Bronze or gold table doesn't exist

**Solution:**
```sql
-- Check table existence
SHOW TABLES IN cloudfastener.xs22xw4aw73q;

-- If missing, verify catalog and schema names
SHOW DATABASES IN cloudfastener;
```

---

### Error: "Column not found: unmapped.WorkflowState"

**Cause:** OCSF table doesn't have `unmapped.WorkflowState` field

**Solution:** Check OCSF schema:
```sql
DESCRIBE TABLE cloudfastener.xs22xw4aw73q.aws_securitylake_sh_findings_2_0;
```

If field is missing, update OCSF transform to use alternative field:
```python
# Fallback: use status field directly
F.col("status").cast("string").alias("finding_status")
```

---

### Error: "No rows found in window"

**Cause:** Bronze table has no data in last 24 hours

**Solution:**
```sql
-- Check data recency
SELECT MAX(cf_processed_time) as latest_data
FROM cloudfastener.xs22xw4aw73q.aws_securityhub_findings_1_0;

-- If data is stale, adjust window or wait for new data
```

**Workaround for testing:** Manually adjust window in Cell 1:
```python
# Override window for testing (e.g., last 7 days)
window_start_ts = window_end_ts - F.expr("INTERVAL 7 DAYS")
```

---

### Gold Table Not Updating

**Cause:** TRUNCATE or INSERT failed silently

**Debug:**
```python
# Add after line 415
print(f"Truncating gold table: {gold_tbl}")
spark.sql(f"TRUNCATE TABLE {gold_tbl}")
print("Truncate successful")

print(f"Writing {gold_count} rows to gold")
gold.write.mode("append").insertInto(gold_tbl)
print("Insert successful")
```

**Verify:**
```sql
SELECT COUNT(*) FROM cloudfastener.xs22xw4aw73q.aws_standard_summary;
-- Should match gold_count from notebook logs
```

---

## Success Criteria

### ✅ Notebook Runs Successfully
- No errors in any cell
- At least 1 company processed successfully
- Summary shows success count > 0

### ✅ Gold Table Populated
- Row count > 0
- `cf_processed_time` = today at 00:00:00 UTC
- `control_pass_score` between 0.0 and 100.0
- `standards_summary` is not NULL

### ✅ CSPM Compliance Verified
- Controls with all suppressed findings have status "NO_DATA"
- Severity aggregation uses rank (critical > high > medium > low)
- Archived findings are excluded
- Count-based logic (no min/max suppression pollution)

### ✅ Matches Security Hub Console
- Overall pass rates within 5% of console
- Standard-level scores are close (timing differences expected)
- Controls with suppressed findings handled correctly

---

## Next Steps After Testing

1. **Schedule Job**: Create Databricks job to run daily
   - Trigger: Cron schedule (e.g., `0 1 * * *` = 1 AM daily)
   - Parameters: `catalog_name=cloudfastener`, `company_id=ALL`
   - Cluster: Use appropriate cluster size

2. **Set Up Alerts**: Monitor for failures
   - Email on job failure
   - Slack notification for skipped companies

3. **Create Dashboard**: Query gold table for reporting
   - Overall compliance scores by account/region
   - Standard-level pass rates
   - Trend over time (if you add historical archival)

4. **Aurora Sync**: Configure daily sync from Databricks to Aurora
   - Query: `SELECT * FROM cloudfastener.*.aws_standard_summary WHERE cf_processed_time = <job_date>`
   - Frequency: After Databricks job completes

---

**Document Version:** 1.0
**Last Updated:** 2025-12-22
**Notebook:** bronze_to_gold_v2.ipynb
