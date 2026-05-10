# Configuration reference

Configure dbt-lint via `dbt-lint.yml` or a `[tool.dbt-lint]` section in `pyproject.toml`. All settings are optional. If no config file is found, dbt-lint uses the defaults shown below.

## Sources and discovery

Resolution order, first match wins (sources are not merged):

1. Explicit `--config PATH` on the command line
2. `pyproject.toml` containing a `[tool.dbt-lint]` section, walking up from cwd
3. `dbt-lint.yml`, walking up from cwd
4. Built-in defaults

`--isolated` skips discovery entirely and uses defaults regardless of ambient config files.

## Full example

```yaml
# Thresholds
documentation_coverage_target: 100
test_coverage_target: 100
models_fanout_threshold: 3
too_many_joins_threshold: 5
chained_views_threshold: 5

# Naming prefixes per model type
staging_prefixes: [stg_]
intermediate_prefixes: [int_]
base_prefixes: [base_]
other_prefixes: [rpt_]

# Marts use plain entity names by default (customers.sql, orders.sql).
# Uncomment to enforce fct_/dim_ prefixes:
# marts_prefixes: [fct_, dim_]

# Expected directory names (string or list)
staging_folder_name: staging
intermediate_folder_name: intermediate
marts_folder_name: marts
base_folder_name: base
# For projects with multiple directories per model type:
# intermediate_folder_name:
#   - intermediate
#   - transformed_intermediate

# Materialization constraints
staging_allowed_materializations: [view]
intermediate_allowed_materializations: [ephemeral, view]
marts_allowed_materializations: [table, incremental]

# Include/exclude resources by file path (regex)
include: null
exclude: null

# Primary key test macros (any combo satisfies the check)
primary_key_test_macros:
  - [dbt.test_unique, dbt.test_not_null]
  - [dbt_utils.test_unique_combination_of_columns]

# Resource types that require primary key tests
enforced_primary_key_node_types: [model]

# Column naming conventions (null = disabled)
# column_naming_conventions:
#   forbidden_suffixes: [_type, _status]
#   boolean_prefixes: [is_, has_, was_]
#   type_suffixes:
#     _at: [timestamp]
#     _date: [date]
#     _id: [integer, bigint]
#     _amt: [numeric, decimal]
#     _cnt: [integer, bigint]

# Column documentation coverage target (null = disabled)
# column_documentation_coverage_target: 80

# Per-rule overrides
rules:
  modeling/too-many-joins:
    severity: error
  structure/intermediate-materialization:
    enabled: false
  testing/sources-without-freshness:
    exclude_resources:
      - source.project.schema.table_name
```

## Per-rule overrides

The `rules:` section accepts any rule ID with the following keys:

| Key | Type | Description |
| --- | --- | --- |
| `enabled` | bool | Disable a rule entirely (`false`) |
| `severity` | string | Override severity: `warn` or `error` |
| `exclude_resources` | list | Resource IDs to skip for this rule |

## Per-resource skip

Opt out of specific rules on individual resources via `meta` in your dbt YAML:

```yaml
# models/staging/stg_legacy_orders.yml
models:
  - name: stg_legacy_orders
    meta:
      dbt-lint:
        skip:
          - modeling/hard-coded-references
          - structure/model-naming-conventions
```

## Suppressions

For existing projects, generate a suppressions file that suppresses all current violations:

```bash
dbt-lint check target/manifest.json --write-suppressions > .dbt-lint-suppressions.yml
```

When `.dbt-lint-suppressions.yml` sits next to the discovered config or in the cwd, `check` applies it automatically. Pass `--suppressions PATH` to point at a different file, or `--isolated` to skip the auto-load.

```bash
dbt-lint check target/manifest.json --suppressions custom-suppressions.yml
```

New violations are still reported. Fix existing violations over time and shrink the suppressions file.
