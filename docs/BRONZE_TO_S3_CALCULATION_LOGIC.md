# Bronze to S3 Pipeline: Calculation Logic & Data Flow

## Overview
The `bronze_to_s3.ipynb` pipeline transforms AWS Security Hub findings from bronze tables into aggregated compliance summaries, calculating scores at multiple levels (region, standard, severity, and account). This document explains the complete calculation logic and data flow.

---

## Pipeline Architecture

```
Bronze Tables (ASFF + OCSF)
    ↓
Finding-Level Transformation & Deduplication
    ↓
Severity Correction (via Reference JSON)
    ↓
Control-Level Aggregation
    ↓
Region/Standard/Severity Summaries
    ↓
Account-Level Summaries
    ↓
S3 CSV Output
```

---

## 1. Input: Bronze Tables

### Tables Processed Per Company
- **ASFF Table**: `<catalog>.<company_id>.aws_securityhub_findings_1_0`
  - Format: AWS Security Finding Format (ASFF) 1.0
  - Source: Security Hub findings from AWS accounts
  
- **OCSF Table**: `<catalog>.<company_id>.aws_securitylake_sh_findings_2_0`
  - Format: Open Cybersecurity Schema Framework (OCSF) 2.0
  - Source: Security Lake (alternative data source)

### Time Window Filter
- **48-hour rolling window** from pipeline execution time
- Filter: `cf_processed_time >= current_timestamp - 48 hours`
- Ensures capture of recent findings regardless of execution schedule

---

## 2. Transformation: Canonical Schema

### ASFF → Canonical
Maps ASFF format to standard schema:
```python
finding_id              → finding.Id
finding_modified_time   → finding.UpdatedAt (ISO8601 → timestamp)
finding_status          → workflow.Status (NEW/NOTIFIED/SUPPRESSED/RESOLVED)
account_id              → finding.AwsAccountId
region_id               → finding.Region
standard_id             → compliance.AssociatedStandards[0].StandardsId
control_id              → compliance.SecurityControlId
compliance_status       → compliance.Status (PASSED/FAILED/WARNING/NOT_AVAILABLE)
severity                → severity.Label (CRITICAL/HIGH/MEDIUM/LOW)
```

### OCSF → Canonical
Maps OCSF format to same canonical schema:
```python
finding_id              → finding_info.uid
finding_modified_time   → finding_info.modified_time_dt
finding_status          → status (IN_PROGRESS → NOTIFIED, others preserved)
account_id              → cloud.account.uid
region_id               → cloud.region
standard_id             → compliance.standards[0]
control_id              → compliance.control
compliance_status       → compliance.status
severity                → severity
```

### Key Transformations
1. **Archived Filtering**: Excludes `RecordState = ARCHIVED` findings
2. **Finding ID Normalization**: Trims whitespace, converts empty to NULL
3. **Timestamp Parsing**: ISO8601 strings → Spark timestamps
4. **Status Mapping**: Normalizes workflow statuses across formats

---

## 3. Deduplication Logic

### Why Deduplication?
- Same finding may appear in both ASFF and OCSF tables
- Findings updated multiple times within 48-hour window
- Need single "latest" record per finding

### Deduplication Window
```python
Window.partitionBy("finding_id")
      .orderBy(
          finding_modified_time.desc_nulls_last(),  # Latest timestamp first
          _preference.desc(),                       # ASFF (1) over OCSF (0)
          _bronze_processed_time.desc_nulls_last()  # Latest ingestion
      )
```

### Priority Order
1. **Most recently modified** finding (by `UpdatedAt` / `modified_time_dt`)
2. **ASFF preferred** over OCSF if timestamps equal
3. **Latest ingestion** if all else equal

**Result**: One row per unique `finding_id`

---

## 4. Severity Correction (Reference Join)

### Problem
Source severity data is often incorrect or missing in Security Hub findings.

### Solution
Join with curated reference file mapping `control_id → correct_severity`.

### Reference File
- **Location**: `/Volumes/cloudfastener/system/job_reference/aws/securityhub_controls.json`
- **Format**: JSON with schema `{control_id: string, severity: string}`
- **Maintained**: AWS Security Hub control definitions

### Join Logic
```python
findings_with_ref = findings.join(
    controls_ref_df.withColumnRenamed("severity", "ref_severity"),
    on="control_id",
    how="left"  # Keep all findings, even if no match
)

# Apply correction
findings.withColumn("severity",
    when(ref_severity.isNotNull(), lower(ref_severity))  # Use reference if available
    .otherwise(lower(severity))                           # Fall back to source
)
```

### Severity Values
- `CRITICAL`: Most severe issues
- `HIGH`: Significant security risks
- `MEDIUM`: Moderate risks
- `LOW`: Minor issues
- `UNCLASSIFIED`: No severity mapping found

**Expected match rate**: >95% (controls should have reference data)

---

## 5. Control-Level Aggregation

### Aggregation Key
One row per unique combination:
```python
(account_id, region_id, standard_id, control_id)
```

### Calculations Per Control

#### Finding Counts
```python
active_cnt    = COUNT(*) WHERE finding_status != 'SUPPRESSED'
failed_cnt    = COUNT(*) WHERE compliance_status = 'FAILED' AND NOT suppressed
passed_cnt    = COUNT(*) WHERE compliance_status = 'PASSED' AND NOT suppressed
unknown_cnt   = COUNT(*) WHERE compliance_status IN ('WARNING', 'NOT_AVAILABLE') AND NOT suppressed
total_cnt     = COUNT(*)  # All findings
```

#### Control Status Logic
Determines overall control status based on finding counts:

```python
control_status = CASE
    WHEN active_cnt = 0                        THEN 'NO_DATA'     # No active findings
    WHEN failed_cnt > 0                        THEN 'FAILED'      # Any failure = control failed
    WHEN unknown_cnt > 0                       THEN 'UNKNOWN'     # No failures, but has unknowns
    WHEN passed_cnt = active_cnt               THEN 'PASSED'      # All findings passed
    ELSE 'UNKNOWN'
END
```

**AWS CSPM Compliance**: This logic matches Security Hub's control status determination.

#### Severity Selection
```python
severity = MAX(severity_rank) WHERE
    severity_rank: CRITICAL=4, HIGH=3, MEDIUM=2, LOW=1, UNCLASSIFIED=0

# Takes the highest severity of all findings for this control
```

**Example**: If control has 3 findings (HIGH, MEDIUM, LOW), control severity = HIGH.

---

## 6. Region/Standard/Severity Summaries

### Aggregation Hierarchy
```
Account + Region
    └── Standards (e.g., CIS, PCI-DSS, AWS Foundational Security)
        └── Severity Levels (CRITICAL, HIGH, MEDIUM, LOW)
            └── Controls
```

### Level 1: Severity-Level Scores
**Aggregation Key**: `(account_id, region_id, standard_id, severity)`

```python
total_controls        = DISTINCT_COUNT(control_id)
total_passed_controls = SUM(CASE WHEN control_status = 'PASSED' THEN 1 ELSE 0 END)

severity_score = ROUND((total_passed_controls / total_controls) * 100, 2)
```

**Output**: Score for each severity level within a standard
- Example: "In PCI-DSS for us-east-1, CRITICAL controls have 85.5% pass rate"

### Level 2: Standard-Level Scores
**Aggregation Key**: `(account_id, region_id, standard_id)`

```python
total_controls        = SUM(total_controls) across all severity levels
total_passed_controls = SUM(total_passed_controls) across all severity levels

standard_score = ROUND((total_passed_controls / total_controls) * 100, 2)
```

**Output**: Overall score for entire standard in a region
- Example: "PCI-DSS in us-east-1 has 82.3% pass rate"

### Level 3: Region-Level Scores
**Aggregation Key**: `(account_id, region_id)`

```python
total_controls        = DISTINCT_COUNT(standard_id, control_id) across all standards
total_passed_controls = SUM(controls WHERE status = 'PASSED')

region_score = ROUND((total_passed_controls / total_controls) * 100, 2)
```

**Output**: Overall compliance score for account+region across ALL standards
- Example: "Account 123456 in us-east-1 has 78.9% overall pass rate"

### Nested JSON Structure
```json
{
  "account_id": "123456789012",
  "region_id": "us-east-1",
  "region_score": 78.90,  // Overall region score
  "total_controls": 323,
  "total_passed_controls": 255,
  "standards_summary": [
    {
      "standard_id": "arn:aws:securityhub:...:standards/pci-dss/v/3.2.1",
      "standard_score": 82.30,  // PCI-DSS overall score
      "controls": {
        "total_controls": 100,
        "total_passed_controls": 82
      },
      "by_severity": [
        {
          "severity": "critical",
          "severity_score": 85.50,  // Critical controls pass rate
          "controls": {
            "total_controls": 20,
            "total_passed_controls": 17
          }
        },
        {
          "severity": "high",
          "severity_score": 80.00,
          "controls": {
            "total_controls": 40,
            "total_passed_controls": 32
          }
        }
        // ... medium, low, unclassified
      ]
    }
    // ... other standards
  ]
}
```

---

## 7. Account-Level Aggregation

### Aggregation Logic
**Aggregation Key**: `(company_index_id, account_id)`

Rolls up all regional data to account level:

```python
total_controls        = SUM(total_controls) across all regions
total_passed_controls = SUM(total_passed_controls) across all regions

account_score = ROUND((total_passed_controls / total_controls) * 100, 2)
```

**Output**: Single compliance score per AWS account across all regions and standards.

### Example Calculation
```
Account 123456789012:
  - us-east-1: 100 controls, 85 passed (85%)
  - us-west-2: 50 controls, 40 passed (80%)
  - eu-west-1: 75 controls, 60 passed (80%)
  
Account-level:
  - total_controls: 225
  - total_passed_controls: 185
  - account_score: (185/225)*100 = 82.22%
```

---

## 8. Output Format

### File Structure
```
s3://bucket/company_index_id/aws/
    ├── region_compliance_summary/YYYY-MM-DD/
    │   └── part-00000.csv  # Regional + standard breakdowns
    └── account_compliance_summary/YYYY-MM-DD/
        └── part-00000.csv  # Account-level rollups
```

### Region Compliance Summary (CSV)
**File**: `region_compliance_summary/YYYY-MM-DD/part-*.csv`

**Columns**:
- `company_index_id`: Company identifier (12-char)
- `job_start_time`: Pipeline execution timestamp (UTC)
- `account_id`: AWS account ID
- `region_id`: AWS region
- `region_score`: Overall compliance score for region (decimal 0-100)
- `total_controls`: Total distinct controls evaluated
- `total_passed_controls`: Controls with PASSED status
- `standards_summary`: **JSON string** with nested standard/severity breakdowns

**Example Row**:
```csv
company_index_id,job_start_time,account_id,region_id,region_score,total_controls,total_passed_controls,standards_summary
xs22xw4aw73q,2026-01-07 08:30:00,123456789012,us-east-1,78.90,323,255,"[{""standard_id"":""arn:aws:...:pci-dss/v/3.2.1"",""standard_score"":82.30,...}]"
```

### Account Compliance Summary (CSV)
**File**: `account_compliance_summary/YYYY-MM-DD/part-*.csv`

**Columns**:
- `company_index_id`: Company identifier
- `job_start_time`: Pipeline execution timestamp
- `account_id`: AWS account ID
- `account_score`: Overall compliance score across all regions (decimal 0-100)
- `total_controls`: Total controls across all regions
- `total_passed_controls`: Passed controls across all regions

**Example Row**:
```csv
company_index_id,job_start_time,account_id,account_score,total_controls,total_passed_controls
xs22xw4aw73q,2026-01-07 08:30:00,123456789012,82.22,225,185
```

---

## 9. Score Interpretation

### Score Naming Convention
- **`region_score`**: Overall score for account+region (all standards combined)
- **`standard_score`**: Score for specific standard within region
- **`severity_score`**: Score for specific severity level within standard
- **`account_score`**: Overall score for account (all regions + all standards)

### Score Calculation Formula
All scores use the same formula:

```
score = (passed_controls / total_controls) * 100
```

Rounded to 2 decimal places (e.g., 82.22).

### Score Ranges
- **90-100%**: Excellent compliance (green)
- **75-89%**: Good compliance (yellow)
- **50-74%**: Fair compliance (orange)
- **0-49%**: Poor compliance (red)

### Control Status Impact on Scores
- ✅ **PASSED**: Counts toward passed_controls (+1 to numerator)
- ❌ **FAILED**: Does not count (+0 to numerator, +1 to denominator)
- ⚠️ **UNKNOWN**: Does not count (+0 to numerator, +1 to denominator)
- 📭 **NO_DATA**: Does not count (+0 to both)

**Important**: UNKNOWN and FAILED both hurt the score equally.

---

## 10. Data Quality & Validation

### Validation Checks

#### Schema Validation
- All required columns must exist in bronze tables
- Missing columns → pipeline fails for that company
- No optional columns (strict validation)

#### Deduplication Metrics
```
Before: 10,000 raw findings from bronze
After:   9,500 unique findings (500 duplicates removed)
```

#### Severity Correction Metrics
```
[DEBUG] Total findings: 9,500
[DEBUG] Matched with reference: 9,025 (95.0%)
[DEBUG] Unmatched (using source): 475 (5.0%)
```

**Expected**: >95% match rate with reference data.

#### Active vs Suppressed Findings
```
Active findings: 9,000
Suppressed findings: 500
```

Suppressed findings excluded from compliance calculations.

### Debug Output
Pipeline logs comprehensive debug information:
1. Reference file loading status
2. Sample control_id mappings
3. Severity distribution before/after correction
4. Match rates for severity join
5. Sample unmatched control_ids (if any)
6. Row counts at each aggregation level

---

## 11. Parallel Processing

### Multithreading Configuration
- **Default**: 4 companies processed simultaneously
- **Configurable**: `MAX_PARALLEL_COMPANIES` parameter
- **Thread-safe**: No shared state, isolated S3 writes

### Processing Flow
```
Company A (Thread 1) ──┐
Company B (Thread 2) ──┤
Company C (Thread 3) ──┼─→ ThreadPoolExecutor → S3 Output
Company D (Thread 4) ──┘
```

### Performance Impact
- **Sequential**: 100 companies @ 2 min each = 200 min
- **Parallel (4 threads)**: 100 companies @ 2 min each ≈ 50 min
- **Speedup**: ~4x faster

### Progress Tracking
```
[PROGRESS] 15/100 companies completed
  ✓ Success: 12  ⊘ Skipped: 2  ✗ Failed: 1
```

---

## 12. Error Handling

### Company-Level Failures
Pipeline continues even if individual companies fail:

```
[FAILED] Company xs22xw4aw73q: Missing REQUIRED column: record_state
[SUCCESS] Company ab34cd56ef78: Processed 1,234 findings
```

### Failure Categories
1. **No bronze tables**: Company has no ASFF/OCSF tables
2. **No data in window**: No findings in 48-hour window
3. **Schema validation**: Missing required columns
4. **No valid findings**: All findings filtered out (archived, null IDs)
5. **Exception**: Unexpected errors (logged with stack trace)

### Pipeline Completion
- ✅ **Success**: If any companies complete successfully
- ⚠️ **Partial Success**: Some companies failed, others succeeded
- ❌ **Complete Failure**: No companies succeeded (rare)

---

## 13. Time Window Rationale

### Why 48 Hours?
1. **Upstream dependency**: S3→Bronze job runs at 00:00 JST (15:00 UTC previous day)
2. **Data availability**: Guarantees at least 2 full days of data
3. **Execution flexibility**: Pipeline can run anytime and still get recent data
4. **Overlap handling**: Deduplication ensures no double-counting

### Window Calculation
```python
window_end   = current_timestamp()
window_start = current_timestamp() - INTERVAL 48 HOURS

# Filter: cf_processed_time >= window_start AND cf_processed_time < window_end
```

### Example Timeline
```
Day 1, 00:00 JST: S3→Bronze job ingests findings (cf_processed_time = Day 1, 15:00 UTC)
Day 1, 00:30 JST: This pipeline runs (window = Day -1 to Day 1)
                  ✓ Captures Day 1 findings
                  ✓ Captures Day 0 findings (still in window)
```

---

## 14. AWS Security Hub Compliance

### CSPM Alignment
This pipeline follows AWS Security Hub CSPM (Cloud Security Posture Management) best practices:

1. **Control-based aggregation**: Findings → Controls → Standards
2. **Status determination**: Failed findings = failed control
3. **Suppression handling**: Suppressed findings excluded
4. **Standard compliance**: Per-standard scoring
5. **Multi-account support**: Account-level rollups

### Compliance Frameworks Supported
- AWS Foundational Security Best Practices
- CIS AWS Foundations Benchmark
- PCI DSS v3.2.1
- NIST 800-53
- ISO 27001
- SOC 2
- Custom standards

---

## 15. Example End-to-End Flow

### Input
```
Company: xs22xw4aw73q
Bronze tables:
  - ASFF: 5,000 findings (48-hour window)
  - OCSF: 3,000 findings (48-hour window)
```

### Processing
```
1. Load & Transform:
   - 5,000 ASFF findings → canonical schema
   - 3,000 OCSF findings → canonical schema
   - Union: 8,000 total

2. Deduplicate:
   - 8,000 → 7,500 unique findings (500 duplicates removed)

3. Severity Correction:
   - Load reference: 323 control mappings
   - Match rate: 7,125/7,500 = 95%
   - 375 unmatched (use source severity)

4. Active Filtering:
   - 7,500 total findings
   - 7,000 active (500 suppressed)

5. Control Aggregation:
   - 7,000 findings → 250 unique controls
   - Control statuses: 200 PASSED, 45 FAILED, 5 UNKNOWN

6. Region Summary:
   - Account 123456, us-east-1
   - 250 controls across 3 standards
   - region_score = (200/250)*100 = 80.00%

7. Account Summary:
   - Account 123456 (2 regions: us-east-1, us-west-2)
   - 450 controls total
   - 360 passed
   - account_score = (360/450)*100 = 80.00%
```

### Output
```
S3 Files:
  xs22xw4aw73q/aws/region_compliance_summary/2026-01-07/part-00000.csv
    → 2 rows (us-east-1, us-west-2)
  
  xs22xw4aw73q/aws/account_compliance_summary/2026-01-07/part-00000.csv
    → 1 row (account 123456)
```

---

## 16. Key Formulas Reference

### Control Status
```python
control_status = CASE
    WHEN COUNT(active) = 0                     THEN 'NO_DATA'
    WHEN COUNT(failed) > 0                     THEN 'FAILED'
    WHEN COUNT(unknown) > 0                    THEN 'UNKNOWN'
    WHEN COUNT(passed) = COUNT(active)         THEN 'PASSED'
    ELSE 'UNKNOWN'
END
```

### Severity Score (per severity within standard)
```python
severity_score = ROUND((passed_controls / total_controls) * 100, 2)

WHERE:
  passed_controls = COUNT(control_id WHERE control_status = 'PASSED')
  total_controls  = COUNT(DISTINCT control_id)
```

### Standard Score (per standard within region)
```python
standard_score = ROUND((passed_controls / total_controls) * 100, 2)

WHERE:
  passed_controls = SUM(passed_controls) across all severities
  total_controls  = SUM(total_controls) across all severities
```

### Region Score (overall for account+region)
```python
region_score = ROUND((passed_controls / total_controls) * 100, 2)

WHERE:
  passed_controls = SUM(passed_controls) across all standards
  total_controls  = DISTINCT_COUNT(standard_id, control_id)
```

### Account Score (overall for account across all regions)
```python
account_score = ROUND((passed_controls / total_controls) * 100, 2)

WHERE:
  passed_controls = SUM(passed_controls) across all regions
  total_controls  = SUM(total_controls) across all regions
```

---

## 17. Troubleshooting Common Issues

### High Unclassified Severity Rate
**Problem**: >10% controls showing as "unclassified"
**Cause**: Reference file not loading or missing control_ids
**Fix**: Check reference file path and contents

### Low Match Rate with Reference
**Problem**: <90% match rate with reference data
**Cause**: control_id format mismatches or outdated reference
**Fix**: Review unmatched control_ids, update reference file

### Empty Output for Company
**Problem**: No CSV files generated
**Causes**:
- No bronze tables exist
- No data in 48-hour window
- All findings archived
- Schema validation failure
**Fix**: Check company bronze tables and data freshness

### Score Seems Too Low
**Problem**: Scores much lower than expected
**Causes**:
- UNKNOWN statuses counting as failures
- Suppressed findings (should be excluded - verify)
- Control aggregation logic (any failure = control fails)
**Fix**: Review control-level data, check finding statuses

---

## Summary

This pipeline implements a comprehensive compliance scoring system that:
1. ✅ Ingests Security Hub findings from multiple formats
2. ✅ Deduplicates and normalizes data
3. ✅ Applies severity corrections from reference data
4. ✅ Aggregates at control/standard/region/account levels
5. ✅ Calculates consistent percentage-based scores
6. ✅ Outputs structured CSV files for downstream consumption
7. ✅ Processes multiple companies in parallel
8. ✅ Handles errors gracefully with comprehensive logging

The calculation logic follows AWS Security Hub CSPM best practices and provides actionable compliance metrics at multiple levels of granularity.
