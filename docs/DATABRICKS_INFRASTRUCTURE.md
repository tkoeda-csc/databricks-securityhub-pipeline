# Databricks Infrastructure Components

## Overview

This document outlines all Databricks infrastructure components required for the Security Hub Standards ETL pipeline, to be provisioned via Terraform.

---

## 1. Unity Catalog Structure

### Catalog

-   **Name**: `<catalog_name>` (e.g., `dev`, `prod`)
-   **Purpose**: Top-level namespace for all company schemas and reference data
-   **Required**: Yes

### Reference Schema

-   **Name**: `reference`
-   **Full Path**: `<catalog_name>.reference`
-   **Purpose**: Centralized reference data shared across all companies
-   **Tables**: None (migrated to JSON file)
-   **Required**: Yes

### Company Schemas (Per Company)

-   **Naming Pattern**: 12-character lowercase alphanumeric (e.g., `xs22xw4aw73q`)
-   **Full Path**: `<catalog_name>.<company_index_id>`
-   **Purpose**: Isolated data namespace per company
-   **Required**: Yes (one per company)

### Bronze Tables (Per Company Schema)

#### ASFF Format Table

-   **Name**: `aws_securityhub_findings_1_0`
-   **Full Path**: `<catalog_name>.<company_index_id>.aws_securityhub_findings_1_0`
-   **Source**: AWS Security Hub findings in ASFF 1.0 format
-   **Schema**:
    -   `finding_id` (string)
    -   `updated_at` (string, ISO8601 timestamp)
    -   `aws_account_id` (string)
    -   `finding_region` (string)
    -   `product_name` (string)
    -   `workflow` (struct: Status)
    -   `compliance` (struct: Status, SecurityControlId, AssociatedStandards)
    -   `severity` (struct: Label)
    -   `record_state` (string: ACTIVE/ARCHIVED)
    -   `cf_processed_time` (timestamp)
-   **Partitioning**: By `cf_processed_time` (recommended)
-   **Required**: Yes (primary data source)

#### OCSF Format Table

-   **Name**: `aws_securitylake_sh_findings_2_0`
-   **Full Path**: `<catalog_name>.<company_index_id>.aws_securitylake_sh_findings_2_0`
-   **Source**: AWS Security Lake findings in OCSF 2.0 format
-   **Schema**:
    -   `finding_info` (struct: uid, modified_time_dt)
    -   `status` (string)
    -   `cloud` (struct: account.uid, region)
    -   `compliance` (struct: status, control, standards)
    -   `severity` (string)
    -   `metadata` (struct: product.name)
    -   `unmapped` (struct: RecordState)
    -   `cf_processed_time` (timestamp)
-   **Partitioning**: By `cf_processed_time` (recommended)
-   **Required**: Yes (fallback data source)

---

## 2. Workspace Files

### Reference Data Files

#### AWS Security Hub Controls Reference

-   **Path**: `/Workspace/Shared/aws_securityhub_controls.json`
-   **Purpose**: Maps control_id to correct severity levels
-   **Format**: JSON Lines or JSON array
-   **Schema**:
    ```json
    {
        "control_id": "string",
        "severity": "string (CRITICAL/HIGH/MEDIUM/LOW)"
    }
    ```
-   **Source**: Created via `create_securityhub_controls_reference.ipynb` notebook
-   **Update Frequency**: As needed when AWS updates Security Hub controls
-   **Required**: Yes (critical for severity classification)

### Notebooks

#### Main ETL Pipeline

-   **Path**: `/Workspace/Repos/<repo>/notebooks/bronze_to_s3.ipynb`
-   **Purpose**: Main ETL pipeline (Bronze → S3 CSV)
-   **Required**: Yes

#### Reference Data Creation

-   **Path**: `/Workspace/Repos/<repo>/notebooks/create_securityhub_controls_reference.ipynb`
-   **Purpose**: Generate aws_securityhub_controls.json file
-   **Required**: Yes (for maintenance)

#### Testing

-   **Path**: `/Workspace/Repos/<repo>/notebooks/test_s3_connection.ipynb`
-   **Purpose**: Validate S3 connectivity and permissions
-   **Required**: No (development only)

---

## 3. Jobs / Workflows

### Security Hub Standards ETL Job

#### Job Configuration

-   **Name**: `security_hub_standards_etl_<env>`
-   **Type**: Notebook Job
-   **Schedule**: Daily at 00:30 JST (15:30 UTC)
    -   Runs 30 minutes after S3→Bronze ingestion completes
    -   Ensures 48-hour rolling window captures fresh data
-   **Timeout**: 4 hours
-   **Max Retries**: 2
-   **Retry on Timeout**: Yes

#### Cluster Configuration

-   **Cluster Type**: Job Cluster (ephemeral)
-   **Databricks Runtime**: 14.3 LTS or later
-   **Worker Type**: Standard_DS3_v2 (or equivalent)
-   **Driver Type**: Standard_DS3_v2 (or equivalent)
-   **Workers**:
    -   Min: 2
    -   Max: 8 (autoscaling enabled)
-   **Auto Termination**: After job completion
-   **Spark Config**:
    -   `spark.sql.session.timeZone = UTC`
    -   `spark.databricks.delta.optimizeWrite.enabled = true`
    -   `spark.databricks.delta.autoCompact.enabled = true`

#### Task Configuration

-   **Task Type**: Notebook
-   **Notebook Path**: `/Workspace/Repos/<repo>/notebooks/bronze_to_s3.ipynb`
-   **Parameters**:
    -   `CATALOG_NAME`: `<catalog_name>` (e.g., `dev`, `prod`)
    -   `COMPANY_INDEX_ID`: `ALL` (process all companies) or specific company ID

#### Notification Settings

-   **On Success**: Email to data-platform@company.com
-   **On Failure**: PagerDuty alert + Email to data-platform@company.com
-   **On Timeout**: PagerDuty alert + Email to data-platform@company.com

---

## 4. Storage / S3 Configuration

### S3 Bucket

-   **Bucket Name**: `<env>-cf-databricks-catalog-bucket`
-   **Region**: Same as Databricks workspace
-   **Purpose**: Store CSV output files for Lambda consumption

### S3 Path Structure

```
s3://<env>-cf-databricks-catalog-bucket/
└── <env>/
    └── dashboard/
        └── csv/
            └── <company_index_id>/
                └── aws/
                    ├── region_compliance_summary/
                    │   └── YYYY-MM-DD/
                    │       └── part-*.csv.gz
                    └── account_compliance_summary/
                        └── YYYY-MM-DD/
                            └── part-*.csv.gz
```

### S3 Access Configuration

-   **Authentication**: IAM Instance Profile or Unity Catalog External Location
-   **Required Permissions**:
    -   `s3:PutObject` (write CSV files)
    -   `s3:GetObject` (read for validation)
    -   `s3:ListBucket` (list existing files)
    -   `s3:DeleteObject` (overwrite mode)

### Unity Catalog External Location (Recommended)

-   **Name**: `s3_csv_output_<env>`
-   **URL**: `s3://<env>-cf-databricks-catalog-bucket/<env>/dashboard/csv/`
-   **Credential**: AWS IAM role with above permissions
-   **Purpose**: Secure, governed S3 access via Unity Catalog

---

## 5. IAM Roles & Policies

### Databricks Job Execution Role

-   **Role Name**: `databricks-job-execution-<env>`
-   **Trust Policy**: Databricks account
-   **Attached Policies**:
    -   Unity Catalog access policy
    -   S3 write access policy
    -   CloudWatch Logs write policy

### S3 Access Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<env>-cf-databricks-catalog-bucket/*",
                "arn:aws:s3:::<env>-cf-databricks-catalog-bucket"
            ]
        }
    ]
}
```

### Unity Catalog Access Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [
                "arn:aws:s3:::<unity-catalog-bucket>/*",
                "arn:aws:s3:::<unity-catalog-bucket>"
            ]
        }
    ]
}
```

---

## 6. Permissions & Grants

### Catalog Permissions

-   **Principal**: Service Principal or User Group (e.g., `data-engineers`)
-   **Grants**:
    -   `USE CATALOG` on `<catalog_name>`
    -   `CREATE SCHEMA` on `<catalog_name>`
    -   `USE SCHEMA` on `<catalog_name>.reference`

### Schema Permissions (Per Company)

-   **Principal**: Service Principal or User Group
-   **Grants**:
    -   `USE SCHEMA` on `<catalog_name>.<company_index_id>`
    -   `SELECT` on all tables in `<catalog_name>.<company_index_id>`

### External Location Permissions

-   **Principal**: Service Principal or User Group
-   **Grants**:
    -   `READ FILES` on `s3_csv_output_<env>`
    -   `WRITE FILES` on `s3_csv_output_<env>`

### Workspace File Permissions

-   **Path**: `/Workspace/Shared/`
-   **Principal**: Service Principal or User Group
-   **Permissions**: Read access to `aws_securityhub_controls.json`

---

## 7. Service Principal (Recommended)

### Purpose

-   Run jobs with service account (not user account)
-   Consistent permissions across environments
-   Easier credential rotation and auditing

### Configuration

-   **Name**: `security-hub-etl-sp-<env>`
-   **Databricks Permissions**:
    -   Workspace access
    -   Cluster creation
    -   Job execution
-   **Unity Catalog Permissions**: See section 6
-   **Cloud Provider Role**: See section 5

---

## 8. Monitoring & Alerting

### Job Monitoring

-   **Platform**: Databricks Job UI + CloudWatch
-   **Metrics**:
    -   Job success/failure rate
    -   Job duration
    -   Data processed (row counts)
    -   Failed/skipped companies

### Data Quality Monitoring

-   **Metrics to Track**:
    -   Severity classification rate (should be >95%)
    -   Control match rate with reference data
    -   Data freshness (cf_processed_time lag)
    -   Schema validation failures

### Alerts

-   **Channels**: PagerDuty (critical), Email (info), Slack (optional)
-   **Conditions**:
    -   Job failure
    -   Job timeout (>4 hours)
    -   > 10% companies failed
    -   Severity match rate <90%

---

## 9. Environment-Specific Configuration

### Development (dev)

-   **Catalog**: `dev`
-   **S3 Bucket**: `dev-cf-databricks-catalog-bucket`
-   **Schedule**: Manual or hourly (testing)
-   **Workers**: 2-4 (smaller scale)

### Staging (stg)

-   **Catalog**: `stg`
-   **S3 Bucket**: `stg-cf-databricks-catalog-bucket`
-   **Schedule**: Daily at 00:30 JST
-   **Workers**: 2-8 (production-like)

### Production (prod)

-   **Catalog**: `prod`
-   **S3 Bucket**: `prod-cf-databricks-catalog-bucket`
-   **Schedule**: Daily at 00:30 JST
-   **Workers**: 2-8 (autoscaling)
-   **High Availability**: Multi-AZ enabled

---

## 10. Terraform Resource Mapping

### Required Terraform Resources

#### Databricks Provider Resources

-   `databricks_catalog` - Unity Catalog
-   `databricks_schema` - Reference schema + company schemas
-   `databricks_storage_credential` - S3 access credential
-   `databricks_external_location` - S3 external location
-   `databricks_grants` - Permissions on catalog/schema/external location
-   `databricks_service_principal` - Service principal for job execution
-   `databricks_job` - ETL job definition
-   `databricks_notebook` - Deploy notebooks (or use Repos)
-   `databricks_workspace_file` - Upload reference JSON file
-   `databricks_cluster_policy` - Standardize cluster configs

#### AWS Provider Resources

-   `aws_s3_bucket` - Output bucket
-   `aws_s3_bucket_versioning` - Enable versioning
-   `aws_s3_bucket_lifecycle_configuration` - Data retention policy
-   `aws_iam_role` - Databricks job execution role
-   `aws_iam_role_policy` - S3 + Unity Catalog permissions
-   `aws_iam_role_policy_attachment` - Attach policies

---

## 11. Data Lifecycle & Retention

### S3 CSV Files

-   **Retention**: 90 days
-   **Lifecycle Policy**:
    -   Move to S3 Infrequent Access: After 30 days
    -   Delete: After 90 days
-   **Versioning**: Enabled (for recovery)

### Bronze Tables

-   **Retention**: Defined by upstream S3→Bronze pipeline
-   **Recommended**: 7-30 days (based on storage costs)

### Reference JSON File

-   **Retention**: Indefinite
-   **Versioning**: Manual (store in Git)
-   **Update Process**: Run `create_securityhub_controls_reference.ipynb` when AWS updates controls

---

## 12. Disaster Recovery

### Backup Strategy

-   **Unity Catalog Metadata**: Backed up by Databricks (automated)
-   **Reference JSON File**: Stored in Git repository
-   **Notebooks**: Stored in Git repository (Repos)
-   **Job Definitions**: Managed by Terraform (state file + Git)
-   **S3 Data**: Versioning enabled (90-day retention)

### Recovery Procedures

1. **Job Failure**: Auto-retry (2 attempts), then manual investigation
2. **Data Corruption**: Rerun job for specific date (parameterized)
3. **Complete Disaster**: Restore from Terraform + Git (RPO: 1 day, RTO: 4 hours)

---

## 13. Cost Optimization

### Compute

-   **Autoscaling**: Enable (2-8 workers)
-   **Spot Instances**: Consider for non-critical jobs (not recommended for prod)
-   **Job Clusters**: Use ephemeral clusters (terminate after job)
-   **Right-sizing**: Monitor job metrics and adjust worker types

### Storage

-   **S3 Lifecycle**: Transition to IA after 30 days
-   **Compression**: Use gzip for CSV files (already implemented)
-   **Partitioning**: Partition bronze tables by `cf_processed_time`
-   **Data Retention**: Delete old data per retention policy

---

## Summary Checklist

### Infrastructure Setup (Terraform)

-   [ ] Unity Catalog created
-   [ ] Reference schema created
-   [ ] Company schemas created (per company)
-   [ ] Bronze tables created (ASFF + OCSF per company)
-   [ ] S3 bucket created with lifecycle policies
-   [ ] IAM roles and policies created
-   [ ] Unity Catalog external location configured
-   [ ] Service principal created with permissions
-   [ ] Job configured with schedule and alerts
-   [ ] Reference JSON file uploaded to workspace

### Validation

-   [ ] Test S3 connectivity (test_s3_connection.ipynb)
-   [ ] Run reference creation notebook
-   [ ] Run ETL pipeline for single company (test)
-   [ ] Run ETL pipeline for all companies (prod)
-   [ ] Verify CSV outputs in S3
-   [ ] Validate severity classification rate (>95%)
-   [ ] Set up monitoring and alerts
-   [ ] Document runbooks for common issues

---

## Related Documentation

-   [bronze_to_s3.ipynb](../notebooks/bronze_to_s3.ipynb) - Main ETL notebook
-   [S3_OUTPUT_SPECIFICATION.md](S3_OUTPUT_SPECIFICATION.md) - Output format details
-   [BRONZE_TO_S3.md](BRONZE_TO_S3.md) - Pipeline architecture
-   [create_securityhub_controls_reference.ipynb](../notebooks/create_securityhub_controls_reference.ipynb) - Reference file creation
