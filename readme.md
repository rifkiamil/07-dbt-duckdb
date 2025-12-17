# Transport Retail dbt DuckDB Project

A dbt project for time-series analytics using DuckDB. This project transforms raw time-series data (UK retail prices and transport usage) into analysis-ready Parquet files.

## What is This Project?

This project uses:
- **dbt (data build tool)** – Transforms raw data into clean, tested, documented tables using SQL
- **DuckDB** – A fast, embedded analytical database (like SQLite, but for analytics)
- **Parquet files** – Columnar file format, great for time-series and analytical workloads

The output is a set of Parquet files organized into:
- **Staging tables** (`stg_*`) – Cleaned raw data
- **Dimension tables** (`dim_*`) – Lookup tables (dates, products, etc.)
- **Fact tables** (`fct_*`) – Metrics and measurements over time

---

## Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd 07-dbt-duckdb
```

### 2. Set Up Python Environment

```bash
# Create and activate a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dbt with DuckDB adapter
pip install dbt-duckdb
```

### 3. Verify Installation

```bash
dbt --version
```

You should see output like:
```
(base) rk@Rs-MacBook-Pro 07-dbt-duckdb % dbt --version
Core:
  - installed: 1.10.16
  - latest:    1.10.16 - Up to date!

Plugins:
  - duckdb: 1.10.0 - Up to date!
```

### 4. Run dbt

```bash
dbt run --profiles-dir .
```

Expected output:
```
(base) rk@Rs-MacBook-Pro 07-dbt-duckdb % dbt run --profiles-dir .
07:07:13  Running with dbt=1.10.16
07:07:14  Registered adapter: duckdb=1.10.0
07:07:15  Found 5 models, 2 sources, 468 macros
07:07:15
07:07:15  Concurrency: 4 threads (target='dev')
07:07:15
07:07:16  1 of 5 START sql external model main.stg_retail_prices ......................... [RUN]
07:07:16  2 of 5 START sql external model main.stg_transport_usage ....................... [RUN]
07:07:16  2 of 5 OK created sql external model main.stg_transport_usage .................. [OK in 0.68s]
07:07:47  1 of 5 OK created sql external model main.stg_retail_prices .................... [OK in 31.34s]
07:07:47  3 of 5 START sql external model main.dim_date .................................. [RUN]
07:07:47  3 of 5 OK created sql external model main.dim_date ............................. [OK in 0.17s]
07:07:47  4 of 5 START sql external model main.fct_retail_prices ......................... [RUN]
07:07:47  5 of 5 START sql external model main.fct_transport_usage ....................... [RUN]
07:07:48  5 of 5 OK created sql external model main.fct_transport_usage .................. [OK in 0.68s]
07:08:32  4 of 5 OK created sql external model main.fct_retail_prices .................... [OK in 44.44s]
07:08:32
07:08:32  Finished running 5 external models in 0 hours 1 minutes and 16.36 seconds (76.36s).
```

### 5. View the Output

Your transformed data is now in the `data/` folder as Parquet files:
```
data/
├── staging/
│   ├── stg_retail_prices.parquet
│   └── stg_transport_usage.parquet
└── marts/
    └── core/
        ├── dim_date.parquet
        ├── fct_retail_prices.parquet
        └── fct_transport_usage.parquet
```

You can query these files directly with Python:
```python
import duckdb

# Query a Parquet file directly
df = duckdb.query("SELECT * FROM 'data/marts/core/fct_retail_prices.parquet' LIMIT 10").df()
print(df)
```

---

## Adding New Raw Data

Found new time-series data on the internet? Here's how to add it.

### Step 1: Create a Folder for Your Raw Data

Place your data file in the `raw/` directory with a descriptive folder name:

```
raw/
├── time-series-uk-retail-supermarket-price-data/
│   └── base_retail_gb_snappy.parquet
├── transport-uk/
│   └── transport-use-statistics.csv
└── your-new-dataset/                          <-- NEW
    └── your_data.csv                          <-- NEW
```

### Step 2: Create a Staging Model

Create a new SQL file in `models/staging/`:

**File: `models/staging/stg_your_new_data.sql`**

```sql
{{
    config(
        materialized='external',
        location='data/staging/stg_your_new_data.parquet',
        format='parquet'
    )
}}

with source as (
    -- For CSV files:
    select * from read_csv_auto('raw/your-new-dataset/your_data.csv')

    -- For Parquet files, use this instead:
    -- select * from read_parquet('raw/your-new-dataset/your_data.parquet')
),

cleaned as (
    select
        -- Cast date columns properly
        cast(date_column as date) as observation_date,

        -- Clean text columns
        trim(category_column) as category,

        -- Cast numeric columns with appropriate precision
        cast(value_column as decimal(10,2)) as metric_value,

        -- Create a surrogate key
        md5(cast(date_column as varchar) || category_column) as record_key
    from source
)

select * from cleaned
```

### Step 3: Add Documentation (Optional but Recommended)

Create a YAML file alongside your model:

**File: `models/staging/stg_your_new_data.yml`**

```yaml
version: 2

models:
  - name: stg_your_new_data
    description: Staging model for your new dataset. Cleans and types the raw data.
    columns:
      - name: record_key
        description: Surrogate key for the record
      - name: observation_date
        description: Date of the observation
      - name: category
        description: Category of the measurement
      - name: metric_value
        description: The measured value
```

### Step 4: Run dbt

```bash
dbt run --profiles-dir .
```

Your new staging model will be built and output to `data/staging/stg_your_new_data.parquet`.

---

## Adding Dimension Tables

Dimension tables contain descriptive attributes for your data (who, what, where, when).

### Example: Creating a New Dimension

**File: `models/marts/core/dim_category.sql`**

```sql
{{
    config(
        materialized='external',
        location='data/marts/core/dim_category.parquet',
        format='parquet'
    )
}}

with categories as (
    select distinct
        category as category_key,
        category as category_name,
        -- Add any additional attributes
        case
            when category like '%food%' then 'Food & Beverage'
            when category like '%transport%' then 'Transportation'
            else 'Other'
        end as category_group
    from {{ ref('stg_your_new_data') }}
)

select * from categories
order by category_name
```

### Key Patterns for Dimensions

- Use `{{ ref('staging_model') }}` to reference your staging tables
- Create a `*_key` column for joining to fact tables
- Add descriptive columns (names, groups, hierarchies)
- Use `distinct` to get unique values

---

## Adding Fact Tables

Fact tables contain your time-series measurements and metrics.

### Example: Creating a New Fact Table

**File: `models/marts/core/fct_daily_metrics.sql`**

```sql
{{
    config(
        materialized='external',
        location='data/marts/core/fct_daily_metrics.parquet',
        format='parquet'
    )
}}

with staging as (
    select * from {{ ref('stg_your_new_data') }}
),

date_dim as (
    select * from {{ ref('dim_date') }}
),

fact_table as (
    select
        -- Keys for joining
        s.record_key,
        s.observation_date as date_key,
        s.category as category_key,

        -- Metrics
        s.metric_value,

        -- Date attributes from dimension
        d.year,
        d.month,
        d.quarter,
        d.is_weekend
    from staging s
    left join date_dim d on s.observation_date = d.date_key
)

select * from fact_table
```

### Key Patterns for Facts

- Include keys (`*_key`) for joining to dimensions
- Include numeric metrics (the "facts")
- Join to `dim_date` to get time-based attributes
- Use `{{ ref() }}` for all table references

---

## Project Structure Explained

```
07-dbt-duckdb/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Database connection settings
├── raw/                     # Raw data files (CSV, Parquet)
│   └── your-dataset/
│       └── data.csv
├── models/
│   ├── staging/             # Staging models (clean raw data)
│   │   ├── sources.yml      # Source definitions
│   │   ├── stg_*.sql        # Staging SQL
│   │   └── stg_*.yml        # Staging documentation
│   └── marts/
│       └── core/            # Dimension and fact tables
│           ├── dim_*.sql    # Dimension tables
│           ├── fct_*.sql    # Fact tables
│           └── *.yml        # Documentation
├── data/                    # Output Parquet files (created by dbt)
│   ├── staging/
│   └── marts/
└── transport_retail_dbt_duck_project.duckdb  # DuckDB database file
```

---

## Useful dbt Commands

```bash
# Run all models
dbt run --profiles-dir .

# Run a specific model
dbt run --profiles-dir . --select stg_your_new_data

# Run a model and all its downstream dependencies
dbt run --profiles-dir . --select stg_your_new_data+

# Test your models (if tests are defined)
dbt test --profiles-dir .

# Generate documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .

# Check for issues
dbt debug --profiles-dir .

# Clean build artifacts
dbt clean --profiles-dir .
```

---

## Understanding the Config Block

Every model file starts with a config block:

```sql
{{
    config(
        materialized='external',
        location='data/staging/your_model.parquet',
        format='parquet'
    )
}}
```

- **`materialized='external'`** – Tells dbt-duckdb to export to a file (not just a database table)
- **`location`** – Path where the Parquet file will be saved
- **`format`** – Output format (`parquet`, `csv`, etc.)

This is what allows us to output standalone Parquet files instead of just database tables.

---

## Reading Raw Data

DuckDB can read CSV and Parquet files directly in SQL:

```sql
-- Read CSV with auto-detected types
select * from read_csv_auto('raw/your-data/file.csv')

-- Read Parquet
select * from read_parquet('raw/your-data/file.parquet')

-- Read multiple files with wildcards
select * from read_parquet('raw/your-data/*.parquet')
```

---

## Tips for Time-Series Data

1. **Always cast dates properly** – Use `cast(your_date as date)` in staging
2. **Use the shared `dim_date`** – Join your facts to `dim_date` for consistent time attributes
3. **Create surrogate keys** – Use `md5()` to create unique identifiers
4. **Keep staging simple** – Clean and type data, don't add business logic
5. **Add business logic in marts** – Joins, calculations, and aggregations go in fact tables

---

## Need Help?

- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [dbt-duckdb Adapter](https://github.com/duckdb/dbt-duckdb)