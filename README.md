# dbt VTEC Charger Data

This dbt project cleans and transforms VTEC charger meter data from a Postgres source into reporting-ready dimension and fact tables.

The pipeline standardizes raw EKM meter readings, removes bad or duplicate records, maps raw meter identifiers to readable names, and builds a trusted fact table for downstream analytics.

## Project Structure

```text
.
├── dbt_project.yml
├── models
│   ├── staging
│   │   ├── _sources.yml
│   │   ├── schema.yml
│   │   ├── stg_postgres__meter_readings.sql
│   │   └── stg_meter_readings_duplicates.sql
│   └── core
│       ├── schema.yml
│       ├── dim_meters.sql
│       └── fact_meter_readings.sql
├── seeds
│   ├── meter_name_lookup.csv
│   └── seeds.yml
└── tests
    ├── assert_no_duplicate_meter_readings.sql
    └── assert_valid_meter_reading_values.sql
```

## Data Flow

1. `ekm_data.ekm_meter_data` is declared as the raw Postgres source.
2. `stg_postgres__meter_readings` standardizes, validates, and deduplicates raw meter readings.
3. `stg_meter_readings_duplicates` captures duplicate records excluded from the main staging model for auditing.
4. `meter_name_lookup.csv` maps raw meter addresses to readable meter names.
5. `dim_meters` builds the meter dimension from the seed lookup.
6. `fact_meter_readings` joins cleaned readings to `dim_meters` for reporting.

## Transformation Steps

The staging layer performs the core cleanup before records are exposed to reporting models:

- Standardizes data types such as timestamps and numeric kWh values.
- Removes duplicate records by keeping the first ranked reading for each meter and start timestamp.
- Filters invalid or incomplete records, including null identifiers, null timestamps, null usage values, negative usage values, and readings where the end timestamp is earlier than the start timestamp.
- Preserves duplicate rows in an audit model so removed records can still be reviewed.
- Adds `dbt_cleaned_at` metadata so analysts can see when dbt last produced the cleaned records.

The core layer maps raw identifiers to structured dimension tables:

- `dim_meters` turns raw `meter_address` values into named meter records.
- `fact_meter_readings` joins staged readings to `dim_meters`, keeping `meter_address` for lineage and relationship testing.
- `fact_meter_readings` adds `dbt_transformed_at` to capture when the reporting table was last produced.

## Models

| Model | Layer | Description |
| --- | --- | --- |
| `stg_postgres__meter_readings` | Staging | Cleaned meter readings with standardized types, invalid rows removed, and duplicates deduplicated. |
| `stg_meter_readings_duplicates` | Staging | Duplicate audit model for records excluded from the main staging model. |
| `dim_meters` | Core | Meter dimension built from the seed lookup table. |
| `fact_meter_readings` | Core | Reporting-ready fact table of validated meter readings joined to meter names. |

## Tests

This project includes dbt schema tests and custom data tests for data quality:

- `not_null` tests on required identifiers, timestamps, meter names, and usage values.
- `unique` test on `dim_meters.meter_address`.
- `relationships` test from `fact_meter_readings.meter_address` to `dim_meters.meter_address`.
- `assert_no_duplicate_meter_readings` to confirm deduplicated staging output.
- `assert_valid_meter_reading_values` to confirm invalid intervals and negative usage do not reach the clean staging layer.

## Transformation Metadata

The project includes two dbt-generated metadata timestamps:

- `dbt_cleaned_at`: the dbt run timestamp for the cleaned staging output.
- `dbt_transformed_at`: the dbt run timestamp for the final reporting fact table.

These fields are useful for lineage, auditability, and troubleshooting. They should not be confused with `start_date_time` or `end_date_time`, which describe the meter reading interval from the source data.

## Getting Started

Install dbt for your target adapter, configure the `vtec` profile, then run:

```bash
dbt deps
dbt seed
dbt build
```

During development, the staging model limits output to 100 rows by default through the `is_test_run` variable. To run without that development limit:

```bash
dbt build --vars '{"is_test_run": false}'
```

To run only the tests:

```bash
dbt test
```

## Lessons Learned

Breaking transformations into smaller pieces makes the pipeline easier to test, debug, document, and maintain. Testing each transformation layer helps identify issues such as null values, duplicates, invalid records, and broken relationships before the data reaches reporting tools. This improves reliability, transparency, and trust in downstream analysis.
