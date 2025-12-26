# Bronze to Silver ETL Notebook Documentation

## Overview
This Databricks notebook implements an ETL pipeline that transforms bronze-layer security findings from AWS Security Hub into a unified silver-layer table. It processes data from two different source formats (ASFF and OCSF) and merges them into a single, deduplicated compliance findings table.

## Purpose
Consolidate AWS Security Hub findings from multiple bronze tables into a single silver table with:
- Unified schema across different source formats
- Deduplication logic that prefers the most recent findings
- Time-windowed processing for incremental updates
- Source preference (OCSF over ASFF when timestamps match)

## Input Parameters

The notebook expects two widget parameters:
- **`company_id`** (required): Identifies the customer/tenant
- **`snapshot_at`** (required): UTC timestamp boundary (e.g., `2025-12-17T00:00:00Z`) that defines the processing window

## Data Sources

### Bronze Tables
1. **ASFF (AWS Security Finding Format)**: `cloudfastener.{company_id}.aws_securityhub_findings_1_0`
   - Original Security Hub format
   - Lower priority in deduplication (preference = 0)

2. **OCSF (Open Cybersecurity Schema Framework)**: `cloudfastener.{company_id}.aws_securitylake_sh_findings_2_0`
   - Newer normalized format
   - Higher priority in deduplication (preference = 1)

### Silver Table
**Output**: `cloudfastener.{company_id}.silver_aws_compliance_findings`

## Processing Logic

### 1. Initialization (Cell 2)
- Validates required parameters
- Sets UTC timezone for consistent timestamp handling
- Calculates processing window: `[snapshot_at - 1 day, snapshot_at)`
- Constructs table names based on company_id

### 2. Helper Functions (Cell 3)
- **`table_exists()`**: Checks if a table exists in the catalog
- **`normalize_finding_id()`**: Trims finding IDs and converts empty strings to NULL
- **`parse_iso8601_to_ts()`**: Converts ISO8601 timestamp strings to Spark timestamps

### 3. Source Data Loading (Cell 4)
- Checks existence of both bronze tables
- Filters data by:
  - Product name = "Security Hub"
  - `cf_processed_time` within the processing window
- Counts rows from each source
- Exits early if no data found in the window

### 4. ASFF Transformation (Cell 5)
Maps ASFF schema to unified format:
- **Status normalization**: Maps ASFF workflow statuses to standardized values
  - NEW → "New"
  - NOTIFIED → "In Progress"
  - SUPPRESSED → "Suppressed"
  - RESOLVED → "Resolved"
- **Compliance fields**: Extracts from `compliance.AssociatedStandards` and `compliance.SecurityControlId`
- **Resource fields**: Takes first element from `resources` array
- **Severity**: Extracts `severity.Label`
- Assigns `_source_preference = 0`

### 5. OCSF Transformation (Cell 6)
Maps OCSF schema to unified format:
- Direct field mappings (already normalized)
- **Resource fields**: Uses coalesce to handle multiple possible locations:
  - `resources[0].uid` / `resources[0].type`
  - `resource.uid` / `resource.type`
  - `unmapped` fields as fallback
- Assigns `_source_preference = 1`

### 6. Union and Filtering (Cell 7)
- Transforms each source using appropriate transformation function
- Filters out rows with NULL `finding_id` early
- Unions all canonical DataFrames
- Logs total union row count

### 7. Deduplication (Cell 8)
Uses a window function to select winner for each `(company_id, finding_id)` pair:
- **Primary sort**: `finding_modified_time` DESC (most recent first)
- **Secondary sort**: `_source_preference` DESC (OCSF over ASFF)
- **Tertiary sort**: `_cf_processed_time` DESC (processing order tie-breaker)
- Keeps only row with `row_number() = 1`

### 8. Merge to Silver (Cell 9)
Performs a MERGE operation:

**MATCHED UPDATE condition** (updates existing rows when):
- Target's `finding_modified_time` is NULL, OR
- Source has newer `finding_modified_time`, OR
- Timestamps match but source has higher preference (OCSF)

**NOT MATCHED INSERT** (inserts new findings):
- Creates new row with all fields
- Sets `created_at` and `updated_at` to `current_timestamp()`

## Key Features

### Deduplication Strategy
1. Prefer most recently modified finding
2. When timestamps match, prefer OCSF over ASFF
3. Use processing time as final tie-breaker

### Incremental Processing
- Processes only data within the 24-hour window
- Uses `cf_processed_time` for window filtering
- Merges incrementally into silver table (upsert pattern)

### Schema Normalization
- Unified column names across both formats
- Consistent timestamp parsing
- Standardized status values
- Nullable fields handled appropriately

### Error Handling
- Validates required parameters
- Checks table existence before querying
- Exits gracefully when no data in window
- Handles NULL finding IDs

## Data Flow Summary

```
Bronze ASFF Table    ──┐
(1-day window)         ├──> Transform to canonical schema
                       │
Bronze OCSF Table    ──┘    ──> Union ──> Deduplicate ──> MERGE ──> Silver Table
(1-day window)
```

## Assumptions & Dependencies
- Both bronze tables must have `cf_processed_time` column
- Tables use UTC timestamps
- Finding IDs are unique identifiers across sources
- At least one bronze table must exist and contain data
- Silver table already exists with matching schema
- Databricks `dbutils` available for widgets and notebook control
