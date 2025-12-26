# Gold Table Schema: aws_standard_summary

## Table Overview

**Full Name:** `cloudfastener.{company_id}.aws_standard_summary`
**Format:** Delta Lake
**Granularity:** One row per (account_id, region_id) combination
**Update Strategy:** Full truncate + append daily (1-day retention)

---

## Schema Definition

### Top-Level Columns

| Column Name         | Data Type | Nullable | Description |
|---------------------|-----------|----------|-------------|
| `company_id`        | STRING    | NO       | Company identifier (12 lowercase alphanumeric) |
| `cf_processed_time` | TIMESTAMP | NO       | Job execution timestamp (00:00:00 UTC on run date) |
| `account_id`        | STRING    | NO       | AWS account ID (12-digit string) |
| `region_id`         | STRING    | NO       | AWS region (e.g., "us-east-1") |
| `control_pass_score` | FLOAT     | NO       | Overall pass rate percentage: `(total_passed / total_rules) * 100` |
| `total_rules`       | INT       | NO       | Total distinct controls across all standards |
| `total_passed`      | INT       | NO       | Count of controls with status "PASSED" |
| `standards_summary` | ARRAY<STRUCT> | NO | Array of per-standard compliance details |

---

## Nested Structure: standards_summary

**Type:** `ARRAY<STRUCT<std, score, controls, controls_by_severity>>`

Each element in the array represents one compliance standard.

### Standard-Level Fields

| Field Name              | Data Type     | Description |
|-------------------------|---------------|-------------|
| `std`                   | STRING        | Standard ARN (e.g., `arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0`) |
| `score`                 | FLOAT         | Standard-level pass rate: `(passed / total) * 100` |
| `controls`              | STRUCT        | Standard-level control counts |
| `controls_by_severity`  | ARRAY<STRUCT> | Per-severity breakdown of control scores |

### controls Field

**Type:** `STRUCT<total: INT, passed: INT>`

| Field   | Data Type | Description |
|---------|-----------|-------------|
| `total` | INT       | Total controls in this standard |
| `passed`| INT       | Controls with status "PASSED" |

**Note:** Controls with status "NO_DATA" are included in `total` but NOT in `passed`.

### controls_by_severity Field

**Type:** `ARRAY<STRUCT<level, score, controls>>`

Each element represents one severity level (critical, high, medium, low, unclassified).

| Field     | Data Type | Description |
|-----------|-----------|-------------|
| `level`   | STRING    | Severity level: `critical`, `high`, `medium`, `low`, `unclassified` |
| `score`   | FLOAT     | Pass rate for this severity: `(passed / total) * 100` |
| `controls`| STRUCT    | Control counts for this severity (same structure as standard-level) |

---

## CREATE TABLE Statement

```sql
CREATE TABLE IF NOT EXISTS cloudfastener.{company_id}.aws_standard_summary (
    company_id STRING COMMENT 'Company identifier (12 chars)',
    cf_processed_time TIMESTAMP COMMENT 'Job execution timestamp (00:00 UTC)',
    account_id STRING COMMENT 'AWS account ID',
    region_id STRING COMMENT 'AWS region',
    control_pass_score FLOAT COMMENT 'Overall pass rate percentage (0-100)',
    total_rules INT COMMENT 'Total distinct controls evaluated',
    total_passed INT COMMENT 'Number of controls with status PASSED',
    standards_summary ARRAY<STRUCT<
        std: STRING COMMENT 'Standard ARN',
        score: FLOAT COMMENT 'Standard pass rate percentage',
        controls: STRUCT<
            total: INT COMMENT 'Total controls in standard',
            passed: INT COMMENT 'Controls with status PASSED'
        >,
        controls_by_severity: ARRAY<STRUCT<
            level: STRING COMMENT 'Severity level',
            score: FLOAT COMMENT 'Severity-level pass rate percentage',
            controls: STRUCT<
                total: INT COMMENT 'Controls at this severity',
                passed: INT COMMENT 'Passed controls at this severity'
            >
        >>
    >> COMMENT 'Per-standard compliance details'
)
USING DELTA
COMMENT 'AWS Security Hub compliance summary by account and region'
LOCATION 's3://your-bucket/gold/{company_id}/aws_standard_summary/';
```

---

## Sample Data

### Example Row (Expanded for Readability)

```json
{
  "company_id": "xs22xw4aw73q",
  "cf_processed_time": "2025-12-22T00:00:00.000Z",
  "account_id": "123456789012",
  "region_id": "us-east-1",
  "control_pass_score": 72.34,
  "total_rules": 156,
  "total_passed": 113,
  "standards_summary": [
    {
      "std": "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
      "score": 72.34,
      "controls": {
        "total": 156,
        "passed": 113
      },
      "controls_by_severity": [
        {
          "level": "critical",
          "score": 85.71,
          "controls": {
            "total": 14,
            "passed": 12
          }
        },
        {
          "level": "high",
          "score": 68.97,
          "controls": {
            "total": 58,
            "passed": 40
          }
        },
        {
          "level": "medium",
          "score": 74.24,
          "controls": {
            "total": 66,
            "passed": 49
          }
        },
        {
          "level": "low",
          "score": 66.67,
          "controls": {
            "total": 18,
            "passed": 12
          }
        }
      ]
    },
    {
      "std": "arn:aws:securityhub:us-east-1::standards/cis-aws-foundations-benchmark/v/1.2.0",
      "score": 81.25,
      "controls": {
        "total": 64,
        "passed": 52
      },
      "controls_by_severity": [
        {
          "level": "high",
          "score": 80.00,
          "controls": {
            "total": 25,
            "passed": 20
          }
        },
        {
          "level": "medium",
          "score": 82.05,
          "controls": {
            "total": 39,
            "passed": 32
          }
        }
      ]
    }
  ]
}
```

---

## Query Examples

### Query 1: Top-Level Summary

```sql
SELECT
    company_id,
    cf_processed_time,
    account_id,
    region_id,
    ROUND(control_pass_score, 2) as pass_score,
    total_rules,
    total_passed,
    SIZE(standards_summary) as num_standards
FROM cloudfastener.xs22xw4aw73q.aws_standard_summary
ORDER BY control_pass_score DESC;
```

**Result:**
| company_id | cf_processed_time | account_id | region_id | pass_rate_pct | total_rules | total_passed | num_standards |
|------------|-------------------|------------|-----------|---------------|-------------|--------------|---------------|
| xs22xw4aw73q | 2025-12-22 00:00 | 123456789012 | us-east-1 | 72.34 | 156 | 113 | 2 |
| xs22xw4aw73q | 2025-12-22 00:00 | 123456789012 | us-west-2 | 68.50 | 200 | 137 | 3 |

---

### Query 2: Explode Standards

```sql
SELECT
    company_id,
    account_id,
    region_id,
    std.std as standard_id,
    std.score as standard_score,
    std.controls.total as total_controls,
    std.controls.passed as passed_controls
FROM (
    SELECT
        company_id,
        account_id,
        region_id,
        explode(standards_summary) as std
    FROM cloudfastener.xs22xw4aw73q.aws_standard_summary
)
ORDER BY standard_score DESC;
```

**Result:**
| company_id | account_id | region_id | standard_id | standard_score | total_controls | passed_controls |
|------------|------------|-----------|-------------|----------------|----------------|-----------------|
| xs22xw4aw73q | 123456789012 | us-east-1 | arn:aws:securityhub:...cis... | 81.25 | 64 | 52 |
| xs22xw4aw73q | 123456789012 | us-east-1 | arn:aws:securityhub:...fsbp... | 72.34 | 156 | 113 |

---

### Query 3: Explode Severity Levels

```sql
SELECT
    company_id,
    account_id,
    region_id,
    standard_id,
    sev.level as severity,
    sev.score as severity_score,
    sev.controls.total as total_controls,
    sev.controls.passed as passed_controls
FROM (
    SELECT
        company_id,
        account_id,
        region_id,
        std.std as standard_id,
        explode(std.controls_by_severity) as sev
    FROM (
        SELECT
            company_id,
            account_id,
            region_id,
            explode(standards_summary) as std
        FROM cloudfastener.xs22xw4aw73q.aws_standard_summary
    )
)
ORDER BY
    company_id,
    account_id,
    region_id,
    standard_id,
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'low' THEN 4
        ELSE 5
    END;
```

**Result:**
| company_id | account_id | region_id | standard_id | severity | severity_score | total_controls | passed_controls |
|------------|------------|-----------|-------------|----------|----------------|----------------|-----------------|
| xs22xw4aw73q | 123456789012 | us-east-1 | arn:...fsbp... | critical | 85.71 | 14 | 12 |
| xs22xw4aw73q | 123456789012 | us-east-1 | arn:...fsbp... | high | 68.97 | 58 | 40 |
| xs22xw4aw73q | 123456789012 | us-east-1 | arn:...fsbp... | medium | 74.24 | 66 | 49 |
| xs22xw4aw73q | 123456789012 | us-east-1 | arn:...fsbp... | low | 66.67 | 18 | 12 |

---

### Query 4: Aggregate Across All Accounts/Regions

```sql
SELECT
    company_id,
    cf_processed_time,
    COUNT(DISTINCT account_id) as num_accounts,
    COUNT(DISTINCT region_id) as num_regions,
    COUNT(*) as total_rows,
    ROUND(AVG(control_pass_score), 2) as avg_pass_score,
    SUM(total_rules) as total_rules_evaluated,
    SUM(total_passed) as total_passed_controls
FROM cloudfastener.xs22xw4aw73q.aws_standard_summary
GROUP BY company_id, cf_processed_time;
```

**Result:**
| company_id | cf_processed_time | num_accounts | num_regions | total_rows | avg_pass_rate | total_rules_evaluated | total_passed_controls |
|------------|-------------------|--------------|-------------|------------|---------------|-----------------------|-----------------------|
| xs22xw4aw73q | 2025-12-22 00:00 | 5 | 8 | 23 | 71.45 | 3458 | 2471 |

---

## Data Characteristics

### Cardinality
- **One row per (account_id, region_id) combination**
- Typical size: 10-50 rows per company (depends on # of accounts/regions)
- Each company has separate gold table

### Data Retention
- **1 day only** (TRUNCATE + append strategy)
- Previous day's data is completely removed
- For historical tracking, sync to external database (e.g., Aurora)

### Update Frequency
- **Daily** (typically run at 1 AM UTC)
- `cf_processed_time` = execution date at 00:00:00 UTC

### Data Volume
- **Top-level:** ~1-2 KB per row
- **With nested arrays:** ~5-10 KB per row (depends on # of standards and severity levels)
- **Total per company:** Typically 50-500 KB

---

## Field Constraints

### NOT NULL Constraints
All top-level fields are NOT NULL:
- `company_id`: Always set from job parameter
- `cf_processed_time`: Always set to job execution time
- `account_id`: Always populated from findings
- `region_id`: Always populated from findings
- `control_pass_score`: Defaults to 0.0 if total_rules = 0
- `total_rules`: Minimum 0
- `total_passed`: Minimum 0
- `standards_summary`: At least 1 element (or row is skipped)

### Value Ranges
- `control_pass_score`: 0.0 to 100.0 (percentage)
- `total_rules`: >= 0
- `total_passed`: 0 to `total_rules`
- `std.score`: 0.0 to 100.0 (already in percentage)
- `sev.score`: 0.0 to 100.0 (already in percentage)

---

## Comparison with Bronze/Silver

| Layer | Granularity | Row Count (per company) | Retention |
|-------|-------------|-------------------------|-----------|
| **Bronze** | Per finding | Millions (all findings) | 90 days |
| **Silver** | N/A (skipped) | N/A | N/A |
| **Gold** | Per account/region | 10-50 rows | 1 day |

**Storage Reduction:** ~99.999% (millions of findings → tens of rows)

---

## Document Version

**Version:** 1.0
**Last Updated:** 2025-12-22
**Pipeline:** bronze_to_gold_v2.ipynb
**Compliance:** AWS Security Hub CSPM scoring logic
