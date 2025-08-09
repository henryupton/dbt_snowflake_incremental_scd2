# dbt_incremental_scd2

A dbt package providing a custom materialization for implementing Slowly Changing Dimension (SCD) Type 2 tables. Optimized for Snowflake.

## Features

- **Custom materialization**: `incremental_scd2` materialization for SCD Type 2 dimensions
- **MERGE-based approach**: Uses Snowflake's MERGE statement for optimal performance
- **Automatic audit columns**: Manages `_IS_CURRENT`, `_VALID_FROM`, `_VALID_TO`, `_UPDATED_AT`, and `_LOADED_AT`
- **Window function logic**: Uses advanced SQL for proper SCD Type 2 versioning
- **Configurable column names**: Customize audit column names at project or model level
- **Snowflake optimized**: Built specifically for Snowflake's SQL dialect and features

## Installation

Add the following to your `packages.yml` file:

```yaml
packages:
  - git: "https://github.com/your-username/dbt_incremental_scd2.git"
    revision: main
```

Then run:
```bash
dbt deps
```

## Demo Workflow

This package includes a complete demo that shows SCD2 behavior across multiple incremental runs:

### Step 1: Initial Load
```bash
# Load seed data and run initial batch
dbt seed
dbt run --vars "batch=1"
```
This creates the initial customer records with SCD2 audit columns.

### Step 2: First Update Batch  
```bash
# Run incremental update with batch 2 data
dbt run --vars "batch=2"
```
This processes:
- Email change for John Smith
- Address change for Jane Doe  
- No changes for Bob Johnson (demonstrates unchanged record handling)
- Name and location change for Alice Brown
- New customer David Miller

### Step 3: Second Update Batch
```bash
# Run incremental update with batch 3 data  
dbt run --vars "batch=3"
```
This processes:
- Address change for John Smith
- Name change for Jane Doe (marriage scenario)
- Name change for Bob Johnson  
- Location change for Alice Brown (move back to Seattle)
- New customer Sarah Davis
- Late-arriving data for customer 8 (demonstrates out-of-order processing)

### View Results
```sql
-- See all SCD2 history
SELECT * FROM customer_scd2 ORDER BY customer_id, _valid_from;

-- See only current records
SELECT * FROM customer_scd2 WHERE _is_current = true;
```

### Batch Scenarios Explained

**Batch 1** (Initial Load):
- 5 customers with initial data
- All records get `_is_current = true`
- `_valid_from` set to `updated_at`
- `_valid_to` set to far future date

**Batch 2** (First Updates):
- John Smith: Email change (creates new version, expires old)
- Jane Doe: Address change 
- Bob Johnson: No changes (no new version created)
- Alice Brown: Name and state change (marriage + move)
- David Miller: New customer (first version)

**Batch 3** (Complex Updates):
- John Smith: Address change (third version)
- Jane Doe: Name change (marriage)
- Bob Johnson: Name change (Robert)
- Alice Brown: Move back to Seattle
- Sarah Davis: New customer
- Customer 8: Late-arriving data (timestamp before others)

## Quick Start

1. Create a model using the `incremental_scd2` materialization:

```sql
{{ 
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id'],
    scd_check_columns=['customer_name', 'email', 'address', 'city', 'state', 'zip_code']
  ) 
}}

with source_data as (
  select 
    customer_id,
    customer_name,
    email,
    address,
    city,
    state,
    zip_code,
    updated_at,
    {{ current_timestamp_func() }} as loaded_at
  from {{ ref('your_source_table') }}
)

select 
  customer_id,
  customer_name,
  email,
  address,
  city,
  state,
  zip_code,
  updated_at as _updated_at,
  loaded_at as _loaded_at
from source_data
```

2. Create a model with specific columns to track for changes:

```sql
{{ 
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id'],
    scd_check_columns=['customer_name', 'email', 'address']
  ) 
}}
```

3. Create a model with custom column names:

```sql
{{ 
  config(
    materialized='incremental_scd2',
    unique_key=['customer_id'],
    is_current_column='current_flag',
    valid_from_column='eff_start_date',
    valid_to_column='eff_end_date',
    updated_at_column='source_timestamp',
    loaded_at_column='etl_timestamp',
    scd_hash_column='change_hash'
  ) 
}}

with source_data as (
  select 
    customer_id,
    customer_name,
    email,
    updated_at,
    {{ current_timestamp_func() }} as loaded_at
  from {{ ref('raw_customers') }}
)

select 
  customer_id,
  customer_name,
  email,
  updated_at as source_timestamp,
  loaded_at as etl_timestamp
from source_data
```

4. Run your model:
```bash
dbt run --models your_model_name
```

## Configuration Options

The `incremental_scd2` materialization supports the following configuration options:

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `unique_key` | ✅ | - | Business key column(s) that uniquely identify a dimension member |
| `incremental_predicates` | ❌ | `[]` | Additional WHERE conditions for the MERGE statement |
| `is_current_column` | ❌ | `_IS_CURRENT` | Name of the current record flag column |
| `valid_from_column` | ❌ | `_VALID_FROM` | Name of the valid from timestamp column |
| `valid_to_column` | ❌ | `_VALID_TO` | Name of the valid to timestamp column |
| `updated_at_column` | ❌ | `_UPDATED_AT` | Name of the source system timestamp column |
| `loaded_at_column` | ❌ | `_LOADED_AT` | Name of the warehouse load timestamp column |
| `scd_hash_column` | ❌ | `_SCD_HASH` | Name of the SCD hash column for change detection |
| `scd_check_columns` | ❌ | `none` | List of columns to include in hash (default: all non-audit columns) |
| `default_valid_to` | ❌ | `2999-12-31 23:59:59+0000` | Default valid_to value for current records |

## Required Columns

Your model must include these audit columns (names are configurable):

| Column Purpose | Default Name | Type | Description |
|---------------|-------------|------|-------------|
| Current Flag | `_IS_CURRENT` | BOOLEAN | Flag indicating if this is the current version of the record |
| Valid From | `_VALID_FROM` | TIMESTAMP | When this version of the record became effective |
| Valid To | `_VALID_TO` | TIMESTAMP | When this version of the record stopped being effective |
| Updated At | `_UPDATED_AT` | TIMESTAMP | Source system timestamp for this record version |
| Loaded At | `_LOADED_AT` | TIMESTAMP | When this record was loaded into the data warehouse |
| SCD Hash | `_SCD_HASH` | STRING | Hash of tracked columns for change detection (via `dbt_utils.surrogate_key`) |

## Available Macros

### `get_scd2_hash(columns, exclude_columns=[])`
Generates an MD5 hash for change detection (optional utility macro).

**Parameters:**
- `columns`: List of column objects to include in hash
- `exclude_columns`: List of column names to exclude from hash

**Example:**
```sql
{{ dbt_incremental_scd2.get_scd2_hash(
  columns=adapter.get_columns_in_relation(ref('source_table')),
  exclude_columns=['updated_at', 'created_at']
) }} as row_hash
```

For surrogate key generation, use `dbt_utils.surrogate_key()`:
```sql
{{ dbt_utils.surrogate_key(['customer_id', '_valid_from']) }} as surrogate_key
```


## How It Works

The `incremental_scd2` materialization handles both initial loads and incremental updates:

### Initial Load (Full Refresh)
When the table doesn't exist or `--full-refresh` is used:
1. **Table Creation**: Creates the target table with your business columns plus audit columns
2. **Audit Columns Added**: Automatically adds the configured audit columns:
   - `_IS_CURRENT`: Set to `true` for all records (they're all current initially)
   - `_VALID_FROM`: Set to the `_updated_at` value from your model
   - `_VALID_TO`: Set to far-future date (2999-12-31)
   - `_UPDATED_AT` and `_LOADED_AT`: Passed through from your model

### Incremental Updates  
For subsequent runs, the strategy uses a sophisticated MERGE statement approach:

1. **Data Preparation**: 
   - Combines new records from the current run with existing records that need updating
   - Uses window functions (`ROW_NUMBER`, `LEAD`) to determine current versions and validity periods

2. **MERGE Logic**:
   - **Matching**: Records are matched on unique key AND `_updated_at` timestamp
   - **UPDATE**: When matched, updates existing records (typically to set `_is_current=false` and proper `_valid_to`)
   - **INSERT**: When not matched, inserts new record versions or completely new records

3. **SCD Type 2 Features**:
   - `_IS_CURRENT`: Set to `true` for the latest version of each business key
   - `_VALID_FROM`: Set to the `_updated_at` timestamp of the record
   - `_VALID_TO`: Set to either the next version's timestamp or far-future date (2999-12-31)
   - `_SCD_HASH`: Hash of tracked columns for efficient change detection (via `dbt_utils.surrogate_key`)
   - Maintains complete history of all changes

### Hash-Based Change Detection
The materialization uses `dbt_utils.surrogate_key` for efficient change detection:
- **Default behavior**: Hashes all non-audit columns 
- **Custom columns**: Use `scd_check_columns` to specify which columns to track
- **Performance**: Only processes records where hash values differ
- **Accuracy**: Detects any change in tracked column values
- **Standardized**: Uses the well-tested `dbt_utils.surrogate_key` macro

## Example Output

For a customer dimension, your table structure will look like:

| customer_id | customer_name | email | _is_current | _valid_from | _valid_to | _updated_at | _loaded_at |
|------------|---------------|-------|-------------|-------------|-----------|-------------|------------|
| 123 | John Smith | john@email.com | false | 2023-01-01 | 2023-06-15 | 2023-01-01 | 2023-01-01 |
| 123 | John Smith | john@newemail.com | true | 2023-06-15 | 2999-12-31 | 2023-06-15 | 2023-06-15 |

## Global Variables

You can customize default audit column names globally by setting variables in your `dbt_project.yml`:

```yaml
vars:
  dbt_incremental_scd2:
    # Default audit column names (can be overridden at model level)
    is_current_column: "current_flag"
    valid_from_column: "eff_start_date"
    valid_to_column: "eff_end_date"
    updated_at_column: "source_timestamp"
    loaded_at_column: "etl_timestamp"
    scd_hash_column: "change_hash"
    # Default valid_to date for current records  
    default_valid_to: "2999-12-31 23:59:59+0000"
```

### Configuration Priority
Configuration values are resolved in the following order (highest to lowest priority):
1. **Model-level config**: Settings in individual model's `config()` block
2. **Project-level variables**: Settings in `dbt_project.yml` vars
3. **Package defaults**: Built-in default values (`_IS_CURRENT`, `_VALID_FROM`, etc.)

## Testing

The package includes comprehensive tests to validate SCD Type 2 behavior:

### Included Tests
- **SCD2 Logic Test**: Ensures each business key has exactly one current record
- **Valid Date Ranges**: Validates that date ranges are logical and current records have far-future end dates
- **No Overlapping Periods**: Ensures no temporal overlaps for the same business key
- **Audit Columns Populated**: Verifies all audit columns are properly populated

### Test Data
- `customers_initial.csv`: Initial customer data for testing
- `customers_updates.csv`: Updated customer data to test incremental behavior

### Running Tests
```bash
# Run all tests
dbt test

# Run only SCD2-specific tests
dbt test --select test_type:generic test_type:singular

# Test a specific model
dbt test --select customer_scd2
```

## Requirements

- dbt >= 1.0.0
- Supported database: Snowflake
- dbt adapter: dbt-snowflake
- dbt_utils package (automatically installed via packages.yml)


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.