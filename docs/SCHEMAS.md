# Schema Reference

Table-level schema documentation for each layer of the Medallion architecture. This document serves as a template -- populate it with your actual table schemas as you build out the platform.

---

## Bronze Schemas

Bronze tables preserve the exact schema from the source system. No transformations are applied -- column names, types, and values arrive as-is.

### Metadata Columns

All Bronze tables include these ingestion metadata columns appended automatically:

| Column | Type | Description |
|---|---|---|
| `_ingested_at` | `timestamp` | UTC timestamp when the row was ingested |
| `_source_file` | `string` | Source file path or API endpoint identifier |
| `_batch_id` | `string` | Unique identifier for the ingestion batch |

### Source Tables

> **Placeholder** -- Add your Bronze source table schemas here. One section per source system.

#### Example: CRM Source

| Table | Source System | Ingestion Method | Approx. Rows |
|---|---|---|---|
| `raw_customers` | CRM | API | ~50,000 |
| `raw_contacts` | CRM | API | ~200,000 |
| `raw_accounts` | CRM | API | ~10,000 |

<details>
<summary><code>raw_customers</code> schema</summary>

| Column | Type | Source Column | Notes |
|---|---|---|---|
| `CustomerID` | `string` | `CustomerID` | Primary key in source |
| `Customer_Name` | `string` | `Customer_Name` | |
| `Created_Date` | `string` | `Created_Date` | Date as string from source |
| `Status` | `string` | `Status` | Active / Inactive |
| `_ingested_at` | `timestamp` | -- | Auto-added |
| `_source_file` | `string` | -- | Auto-added |

</details>

---

## Silver Schemas

Silver tables contain cleansed, deduplicated, and standardised data. Schemas are defined in YAML configs at `datalake/inputs/silver/`.

### Source Tables (Silver)

Silver source tables are 1:1 cleanses of Bronze tables with standardised column names, consistent types, and null handling.

> **Placeholder** -- Add your Silver source table schemas here.

#### Example: Customer (Silver)

| Column | Type | Source (Bronze) | Cleaning Applied |
|---|---|---|---|
| `customer_id` | `int` | `raw_customers.CustomerID` | Cast to int |
| `customer_name` | `string` | `raw_customers.Customer_Name` | Trim, null_if_empty |
| `created_date` | `date` | `raw_customers.Created_Date` | Parse date |
| `status` | `string` | `raw_customers.Status` | Lower, trim |
| `_loaded_at` | `timestamp` | -- | Delta-Gen auto-added |
| `_hash` | `string` | -- | Change detection hash |

### Transform Tables (Silver)

Silver transform tables combine or reshape source data for downstream consumption. These may join multiple Bronze/Silver tables or apply business logic.

> **Placeholder** -- Add transform table schemas here as needed.

---

## Gold Schemas

Gold tables form a star schema dimensional model optimised for analytics and Power BI.

### Dimension Tables

Dimension tables are prefixed with `d_` and contain a surrogate key (`{table}_key`), a business key (`{entity}_id`), and descriptive attributes.

> **Placeholder** -- Add your dimension table schemas here.

#### Example: d_customer

| Column | Type | Description |
|---|---|---|
| `customer_key` | `int` | Surrogate key (auto-generated) |
| `customer_id` | `int` | Business key from source |
| `customer_name` | `string` | Customer display name |
| `status` | `string` | Current status |
| `created_date` | `date` | Customer creation date |
| `_valid_from` | `timestamp` | SCD2 validity start |
| `_valid_to` | `timestamp` | SCD2 validity end |
| `_is_current` | `boolean` | Current record flag |

#### Example: d_date

| Column | Type | Description |
|---|---|---|
| `date_key` | `int` | Date in YYYYMMDD format |
| `full_date` | `date` | Calendar date |
| `year` | `int` | Calendar year |
| `quarter` | `int` | Calendar quarter (1-4) |
| `month` | `int` | Calendar month (1-12) |
| `month_name` | `string` | Month name (January, etc.) |
| `day_of_week` | `int` | Day of week (1=Monday) |
| `day_name` | `string` | Day name (Monday, etc.) |
| `is_weekend` | `boolean` | Weekend flag |
| `fiscal_year` | `int` | Fiscal year |
| `fiscal_quarter` | `int` | Fiscal quarter |

### Fact Tables

Fact tables are prefixed with `f_` and contain foreign keys to dimensions plus numeric measures.

> **Placeholder** -- Add your fact table schemas here.

#### Example: f_transactions

| Column | Type | Description |
|---|---|---|
| `transaction_key` | `long` | Surrogate key |
| `customer_key` | `int` | FK to d_customer |
| `product_key` | `int` | FK to d_product |
| `date_key` | `int` | FK to d_date |
| `transaction_id` | `string` | Business key from source |
| `quantity` | `int` | Transaction quantity |
| `amount` | `decimal(18,2)` | Transaction amount |
| `_loaded_at` | `timestamp` | Load timestamp |

### Sentinel Records

Each dimension includes sentinel records to handle missing foreign key lookups:

| Sentinel | Key Value | Used When |
|---|---|---|
| `NO_CUSTOMER` | `-1` | Fact row has no matching customer |
| `NO_PRODUCT` | `-2` | Fact row has no matching product |
| `NO_DATE` | `19000101` | Fact row has no valid date |

---

## Logging Schemas

Logging and observability tables live in the `{env}-log` lakehouse.

### pipeline_runs

| Column | Type | Description |
|---|---|---|
| `run_id` | `string` | Unique pipeline run identifier |
| `pipeline_name` | `string` | Name of the pipeline or notebook |
| `layer` | `string` | bronze / silver / gold |
| `status` | `string` | SUCCESS / FAILED / RUNNING |
| `start_time` | `timestamp` | Run start time (UTC) |
| `end_time` | `timestamp` | Run end time (UTC) |
| `duration_seconds` | `int` | Total duration |
| `error_message` | `string` | Error details (if failed) |
| `batch_name` | `string` | daily / weekly / monthly |

### dq_results

| Column | Type | Description |
|---|---|---|
| `run_id` | `string` | Pipeline run identifier |
| `run_date` | `date` | Date of the DQ check |
| `table_name` | `string` | Table being checked |
| `check_name` | `string` | Name of the DQ check (e.g., `not_null`, `unique`) |
| `column_name` | `string` | Column being checked (if applicable) |
| `check_result` | `string` | PASS / FAIL / WARN |
| `details` | `string` | Additional context (e.g., null count, duplicate count) |

### dq_summary

| Column | Type | Description |
|---|---|---|
| `run_id` | `string` | Pipeline run identifier |
| `run_date` | `date` | Date of the DQ check |
| `table_name` | `string` | Table being checked |
| `total_checks` | `int` | Number of DQ checks run |
| `passed` | `int` | Checks that passed |
| `failed` | `int` | Checks that failed |
| `warnings` | `int` | Checks that warned |

### row_counts

| Column | Type | Description |
|---|---|---|
| `run_date` | `date` | Date of the count |
| `table_name` | `string` | Logical table name |
| `bronze_count` | `long` | Row count in Bronze |
| `silver_count` | `long` | Row count in Silver |
| `gold_count` | `long` | Row count in Gold |
