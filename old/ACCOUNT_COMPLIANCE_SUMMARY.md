# Account Compliance Summary Implementation

## Overview
Added a new account-level aggregation table to the bronze_to_gold_v2 pipeline.

## Table Structure

### `cloudfastener.{company_id}.aws_account_compliance_summary`

| Column Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `company_id` | STRING | 企業識別子 | xs22xw4aw73q |
| `cf_processed_at` | TIMESTAMP | 集計日 (Job実行日時) | 2024-01-01 00:00:00.000+00:00 |
| `account_id` | STRING | AWSアカウントID | 123456789012 |
| `score` | FLOAT | 総合コンプライアンススコア (0-100) | 80.0000000 |
| `total_rules` | INTEGER | 全リージョン合計コントロール数 | 200 |
| `total_passed` | INTEGER | 全リージョン合計合格数 | 160 |

## Calculation Logic

### Aggregation Flow
```
aws_standard_summary (region-level)
    ↓
    GROUP BY account_id (sum across all regions)
    ↓
aws_account_compliance_summary (account-level)
```

### Score Calculation
```python
score = (total_passed / total_rules) * 100
```
- **total_rules**: Sum of `total_rules` from all regions for that account
- **total_passed**: Sum of `total_passed` from all regions for that account
- **score**: Percentage rounded to 7 decimal places

## Implementation Details

### Location in Pipeline
The account summary is created after the regional `aws_standard_summary` table is populated, within the same `process_company()` function.

### Key Features
1. **Automatic Creation**: Table is created automatically if it doesn't exist
2. **1-day Retention**: Uses TRUNCATE + Append strategy (same as regional table)
3. **Cross-Region Aggregation**: Sums metrics from all regions per account
4. **Japanese Comments**: Column comments in Japanese for clarity
5. **Delta Format**: Uses Delta Lake for ACID compliance

### Code Flow
```python
# 1. Create regional gold table (aws_standard_summary)
gold = aggregate_to_gold(controls, company_id, cf_processed_time)

# 2. Aggregate to account level
account_summary = (
    gold
    .groupBy("company_id", "cf_processed_time", "account_id")
    .agg(
        F.sum("total_rules").alias("total_rules"),
        F.sum("total_passed").alias("total_passed")
    )
    .withColumn("score", (F.col("total_passed") / F.col("total_rules")) * 100)
)

# 3. Write to table
account_summary.write.mode("append").insertInto(account_summary_tbl)
```

## Usage Example

### Query Account Compliance
```sql
SELECT
    account_id,
    score,
    total_rules,
    total_passed,
    cf_processed_at
FROM cloudfastener.xs22xw4aw73q.aws_account_compliance_summary
WHERE cf_processed_at = '2024-01-01'
ORDER BY score DESC;
```

### Compare Accounts
```sql
SELECT
    account_id,
    ROUND(score, 2) as compliance_percentage,
    total_passed || '/' || total_rules as pass_rate
FROM cloudfastener.xs22xw4aw73q.aws_account_compliance_summary
ORDER BY score ASC;
```

## Data Lineage

```
Bronze Tables
  ├─ aws_securityhub_findings_1_0 (ASFF)
  └─ aws_securitylake_sh_findings_2_0 (OCSF)
       ↓
  [Transform & Deduplicate]
       ↓
  [Join with securityhub_controls reference]
       ↓
  [Aggregate to Control-level]
       ↓
Gold Table (Regional)
  └─ aws_standard_summary
       ↓
  [Sum by Account]
       ↓
Account Summary Table
  └─ aws_account_compliance_summary ← NEW!
```

## Testing

### Verify Table Creation
```python
# Check if table exists
spark.catalog.tableExists("cloudfastener.xs22xw4aw73q.aws_account_compliance_summary")

# Show table schema
spark.sql("DESCRIBE cloudfastener.xs22xw4aw73q.aws_account_compliance_summary").show()
```

### Validate Data
```python
# Count records
df = spark.table("cloudfastener.xs22xw4aw73q.aws_account_compliance_summary")
print(f"Records: {df.count()}")

# Show sample
df.show(10, truncate=False)

# Verify score calculation
df.withColumn(
    "calculated_score",
    F.round((F.col("total_passed") / F.col("total_rules")) * 100, 7)
).select("account_id", "score", "calculated_score").show()
```

## Benefits

1. **Simplified Reporting**: Single row per account (instead of per region)
2. **Cross-Region View**: Holistic account compliance across all regions
3. **Performance**: Pre-aggregated for fast queries
4. **Consistency**: Uses same retention and update strategy as regional table

## Notes

- The table is populated automatically during each bronze_to_gold job run
- Each account will have one row per job execution date
- Score precision is set to 7 decimal places to match requirements
- The table follows the same 1-day retention strategy (TRUNCATE + Append)
