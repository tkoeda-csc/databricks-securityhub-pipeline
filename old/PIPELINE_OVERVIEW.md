# AWS Security Hub Compliance Pipeline Overview

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Pipeline Features](#pipeline-features)
3. [AWS Security Hub CSPM](#aws-security-hub-cspm)
4. [Data Flow](#data-flow)
5. [Expected Delays & Characteristics](#expected-delays--characteristics)
6. [Use Cases & Limitations](#use-cases--limitations)

---

## System Architecture

### High-Level Flow

```
AWS Security Hub (CSPM Checks)
    ↓ (Export)
AWS S3 (Security Hub Findings)
    ↓ (Ingestion - 24h)
Bronze Tables (ASFF + OCSF Formats)
    ↓ (ETL - bronze_to_gold_v2.ipynb)
Gold Table (Aggregated Compliance Summary)
    ↓ (Query/Reporting)
Dashboards / APIs / Reports
```

### Components

| Component | Purpose | Technology | Update Frequency |
|-----------|---------|------------|------------------|
| **AWS Security Hub** | Runs compliance checks on AWS resources | AWS managed service | 12-24 hours (periodic) or change-triggered |
| **S3 Export** | Stores Security Hub findings | AWS S3 | Continuous (as findings update) |
| **Bronze Ingestion** | Loads findings from S3 to Databricks | Custom pipeline | Every 24 hours |
| **Bronze Tables** | Raw findings storage (ASFF + OCSF) | Delta Lake | Updated every 24 hours |
| **ETL Pipeline** | Transforms bronze → gold | bronze_to_gold_v2.ipynb | Daily (typically 1 AM UTC) |
| **Gold Table** | Aggregated compliance summaries | Delta Lake | Replaced daily |

---

## Pipeline Features

### Core Capabilities

**1. Direct Bronze → Gold Transformation**
- No intermediate silver layer (saves storage, faster processing)
- In-memory deduplication and aggregation
- Optimized for current-state compliance reporting

**2. Dual-Format Support**
- **ASFF (AWS Security Finding Format)**: Primary format, higher priority
- **OCSF (Open Cybersecurity Schema Framework)**: Secondary format, fallback
- Automatic deduplication when same finding exists in both formats

**3. AWS Security Hub CSPM Compliance**
- Matches Security Hub control status calculation exactly
- Properly handles suppressed and archived findings
- Implements control status precedence: NO_DATA → FAILED → UNKNOWN → PASSED

**4. Multi-Company Processing**
- Auto-discovers all companies in catalog
- Per-company error isolation (one failure doesn't stop others)
- Processes sequentially with summary reporting

**5. Latest-State Design**
- Gold table stores only current day's data (TRUNCATE + Append)
- No historical tracking (optimized for "what's the compliance state now?")
- Suitable for dashboards, not auditing/trending

### Key Transformations

**Deduplication**:
- Groups by `finding_id`
- Prefers: Most recent → ASFF over OCSF → Latest ingestion

**Aggregation Hierarchy**:
1. **Finding-level** → **Control-level** (by control_id, account, region)
2. **Control-level** → **Severity-level** (by severity within standard)
3. **Severity-level** → **Standard-level** (by standard ARN)
4. **Standard-level** → **Account/Region summary**

**Control Status Calculation** (count-based):
```
active_cnt = non-suppressed findings
failed_cnt = active findings with FAILED status
passed_cnt = active findings with PASSED status
unknown_cnt = active findings with WARNING/NOT_AVAILABLE status

control_status =
  IF active_cnt == 0 THEN "NO_DATA"
  ELSE IF failed_cnt > 0 THEN "FAILED"
  ELSE IF unknown_cnt > 0 THEN "UNKNOWN"
  ELSE IF passed_cnt == active_cnt THEN "PASSED"
  ELSE "UNKNOWN"
```

---

## AWS Security Hub CSPM

### What is Security Hub CSPM?

**AWS Security Hub Cloud Security Posture Management (CSPM)** continuously evaluates AWS resources against security standards (CIS, FSBP, PCI DSS, etc.) and generates findings for non-compliant resources.

### How Security Hub Works

**1. Enable Security Standards**
- You enable standards (e.g., AWS Foundational Security Best Practices)
- Security Hub creates checks for all controls in the standard

**2. Initial Check Run**
- First check runs within **2 hours** (most within 25 minutes)
- Controls show "No data" until first check completes
- New standards with shared rules may take **up to 24 hours** for first findings

**3. Ongoing Checks**

**Periodic Checks** (12 or 24 hours):
- Automatically run on a schedule
- Used when resource doesn't support change detection
- Example: KMS.4 (KMS key rotation) - runs every 12 hours

**Change-Triggered Checks**:
- Run when resource state changes (via AWS Config)
- Used whenever possible (most controls)
- With **continuous recording**: Immediate (within minutes)
- With **daily recording**: Up to 24 hours delay
- **Safety check**: Every 18 hours to catch missed updates

### Finding Workflow States

| Workflow Status | Meaning | Included in Control Status? |
|-----------------|---------|---------------------------|
| **NEW** | New finding, not reviewed | ✅ Yes |
| **NOTIFIED** | Sent to external system | ✅ Yes |
| **RESOLVED** | Issue fixed | ✅ Yes (but compliance status changes to PASSED) |
| **SUPPRESSED** | User chose to suppress | ❌ No (excluded from control status) |

| Record State | Meaning | Included in Pipeline? |
|--------------|---------|----------------------|
| **ACTIVE** | Current finding | ✅ Yes |
| **ARCHIVED** | No longer applicable (resource deleted, control disabled) | ❌ No (filtered out) |

### Control Status Rules (AWS Official)

From Security Hub documentation:

> - **Passed** – All findings for the control have a compliance status of PASSED
> - **Failed** – At least one finding has a compliance status of FAILED
> - **Unknown** – At least one finding has a compliance status of WARNING or NOT_AVAILABLE, and no findings have a compliance status of FAILED
> - **No data** – No findings for the control, or all findings are SUPPRESSED or ARCHIVED

**Our pipeline implements these rules exactly.**

---

## Data Flow

### Stage 1: AWS Security Hub → S3

**Frequency**: Continuous (as findings update)

**Process**:
1. Security Hub runs checks (periodic or change-triggered)
2. Findings created/updated in Security Hub
3. Findings exported to S3 bucket

**Format**:
- ASFF (AWS Security Finding Format) - primary
- OCSF (Open Cybersecurity Schema Framework) - optional

**Lag**: Near real-time (minutes after check completes)

---

### Stage 2: S3 → Bronze Tables

**Frequency**: Every 24 hours

**Process**:
1. Ingestion job reads findings from S3
2. Writes to bronze Delta tables in Databricks
3. Adds `cf_processed_time` = ingestion timestamp (UTC)

**Tables Created**:
- `cloudfastener.{company_id}.aws_securityhub_findings_1_0` (ASFF)
- `cloudfastener.{company_id}.aws_securitylake_sh_findings_2_0` (OCSF)

**Lag**: Up to 24 hours

---

### Stage 3: Bronze → Gold (ETL Pipeline)

**Frequency**: Daily (typically 1 AM UTC)

**Process** (bronze_to_gold_v2.ipynb):
1. **Load**: Read bronze tables (24-hour window)
2. **Filter**: Exclude archived findings (`RecordState != "ARCHIVED"`)
3. **Transform**: Map ASFF + OCSF to canonical schema
4. **Union**: Combine both formats
5. **Deduplicate**: Keep most recent by `finding_id` (prefer ASFF)
6. **Mark Suppressed**: Add boolean flag (`is_suppressed`)
7. **Aggregate**: 4-level hierarchy (control → severity → standard → account/region)
8. **Write**: TRUNCATE + Append to gold table

**Table Created**:
- `cloudfastener.{company_id}.aws_standard_summary`

**Lag**: Depends on job schedule (typically runs at 1 AM UTC)

**Output Schema**:
```
company_id              STRING
cf_processed_time       TIMESTAMP
account_id              STRING
region_id               STRING
control_pass_score      FLOAT      (0.0-100.0 percentage)
total_rules             INT        (count of all controls)
total_passed            INT        (count of PASSED controls)
standards_summary       ARRAY<STRUCT<
  std                   STRING     (standard ARN)
  score                 FLOAT      (0.0-100.0)
  controls              STRUCT<total, passed>
  controls_by_severity  ARRAY<STRUCT<level, score, controls>>
>>
```

---

## Expected Delays & Characteristics

### End-to-End Latency

**Best Case** (change-triggered with continuous recording):
```
Resource Change → 0-5 min (Security Hub check)
Security Hub → S3 → 1-5 min
S3 → Bronze → 0-24 hours (daily ingestion)
Bronze → Gold → 0-24 hours (daily ETL)
Total: Up to 48 hours
```

**Worst Case** (periodic check + daily recording):
```
Resource Change → 0-24 hours (daily recording)
Security Hub Check → 12-24 hours (periodic)
Security Hub → S3 → 1-5 min
S3 → Bronze → 0-24 hours (daily ingestion)
Bronze → Gold → 0-24 hours (daily ETL)
Total: Up to 72 hours
```

**Typical** (most controls):
```
Resource Change → 1-2 hours (change-triggered)
S3 → Bronze → 12-24 hours
Bronze → Gold → 12-24 hours
Total: 24-48 hours
```

### Delay Breakdown by Component

| Stage | Min Delay | Max Delay | Typical | Frequency |
|-------|-----------|-----------|---------|-----------|
| Security Hub Check | 0 min (change) | 24h (periodic) | 1-12h | Varies by control |
| Security Hub → S3 | 1 min | 5 min | 2 min | Continuous |
| S3 → Bronze | 0h | 24h | 12h | Every 24 hours |
| Bronze → Gold | 0h | 24h | 1h | Daily at 1 AM UTC |
| **Total** | **1 min** | **73h** | **24-48h** | - |

### Data Freshness

**What "current compliance state" means in gold table**:
- "Compliance state based on Security Hub checks that ran **24-48 hours ago**"
- Not real-time resource state
- Suitable for daily/weekly reporting, not real-time alerts

**Example Timeline**:
```
Monday 9 AM:  Resource becomes non-compliant (S3 bucket made public)
Monday 10 AM: Security Hub check runs (change-triggered)
Monday 10:05: Finding exported to S3
Tuesday 1 AM: Ingestion job loads to bronze
Wednesday 1 AM: ETL job aggregates to gold
Wednesday 9 AM: Dashboard shows the issue (48 hours after incident)
```

### Data Retention

| Layer | Retention | Reason |
|-------|-----------|--------|
| **Security Hub** | 90 days (AWS default) | AWS managed |
| **S3** | Configurable (typically 90-365 days) | Long-term archive |
| **Bronze** | Configurable (typically 30-90 days) | Source of truth in Databricks |
| **Gold** | **1 day only** (latest state) | Current compliance only |

**Implication**: Gold table is **not suitable for historical trending**. Use bronze or S3 for historical analysis.

---

## Use Cases & Limitations

### ✅ Good Use Cases

**1. Current Compliance Dashboard**
- "What's our compliance posture today?"
- Acceptable with 24-48h lag
- Example: Executive dashboard showing pass rates by account/region

**2. Daily/Weekly Reporting**
- "Send weekly compliance report to leadership"
- Gold table refreshed daily is sufficient

**3. Compliance Metrics**
- "What % of controls are passing?"
- "Which accounts have lowest pass rates?"
- "What's our average severity score?"

**4. Standard Comparison**
- "How do we perform on CIS vs FSBP?"
- "Which standard has most failures?"

### ❌ Limitations

**1. Not Real-Time**
- ❌ Can't use for: Real-time security alerts
- ✅ Use Security Hub EventBridge for real-time alerting

**2. No Historical Trending**
- ❌ Can't query: "What was our pass rate 30 days ago?"
- ✅ Would need to sync gold table to external database daily

**3. No Finding-Level Detail**
- ❌ Can't query: "Which S3 buckets are non-compliant?"
- ✅ Use bronze tables for finding-level queries

**4. No Audit Trail**
- ❌ Can't answer: "When did this control first fail?"
- ✅ Use bronze or S3 for audit requirements

### Workarounds for Limitations

**Real-Time Alerts**:
- Use Security Hub → EventBridge → Lambda/SNS for real-time notifications
- Keep gold table for reporting/trending

**Historical Tracking**:
- Add daily sync job: Gold → Aurora/PostgreSQL/Time-series DB
- Store daily snapshots with date partition

**Finding-Level Queries**:
- Query bronze tables directly
- Create silver layer if needed (finding-level, deduplicated)

**Audit Trail**:
- Enable S3 versioning on Security Hub exports
- Query bronze tables for historical findings

---

## Architecture Trade-offs

### Chosen Design

**Optimized For**:
- ✅ Current-state compliance reporting
- ✅ Low storage cost (no silver, 1-day gold)
- ✅ Fast processing (in-memory, no intermediate writes)
- ✅ Simple pipeline (fewer moving parts)

**Trade-offs Accepted**:
- ❌ No real-time data
- ❌ No historical trending
- ❌ No finding-level queries in gold
- ❌ 24-48 hour lag

### Alternative Designs

**For Real-Time Compliance**:
- Add: Security Hub → EventBridge → Lambda → Gold (direct)
- Update gold on every finding change (expensive, complex)

**For Historical Trending**:
- Add: Daily gold snapshot → Time-series database
- Keep 365 days of daily summaries

**For Finding-Level Queries**:
- Add: Silver layer (deduplicated findings)
- Update silver every 6-12 hours
- Query silver for finding details, gold for summaries

---

## Summary

**This pipeline is designed for**:
- Organizations that need **daily compliance reporting**
- Acceptable **24-48 hour lag** behind real-time resource state
- Current compliance posture > Historical trending
- Cost optimization (no silver, 1-day gold retention)

**Not designed for**:
- Real-time security incident response
- Compliance audits requiring historical evidence
- Finding-level detail in reporting layer
- Sub-hourly reporting frequency

**Key Insight**: This is a **reporting pipeline**, not an alerting system. For real-time security, use Security Hub native alerting (EventBridge) alongside this pipeline.

---

**Document Version**: 1.0
**Last Updated**: 2025-12-22
**Related Documents**:
- [bronze_to_gold_v2.md](bronze_to_gold_v2.md) - Pipeline logic details
- [bronze_to_gold_v2_compliance.md](bronze_to_gold_v2_compliance.md) - CSPM compliance rules
- [GOLD_SCHEMA.md](GOLD_SCHEMA.md) - Output schema documentation
- [TESTING_GUIDE.md](TESTING_GUIDE.md) - Testing instructions
