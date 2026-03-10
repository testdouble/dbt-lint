# dbt-linter

Manifest-only semantic linter for dbt projects. Analyzes `manifest.json` to enforce DAG structure, naming conventions, test coverage, documentation standards, and governance rules. No dbt runtime or warehouse connection required.

## Install

```bash
# Add to a project
uv add git+https://github.com/yourusername/dbt-linter.git

# Or install directly
uv pip install git+https://github.com/yourusername/dbt-linter.git

# From a local clone
uv pip install -e .
```

Requires Python 3.11+. Two runtime dependencies: `pyyaml`, `click`.

## Quick start

```bash
# Generate manifest (requires dbt)
dbt parse  # or dbt compile / dbt run

# Lint
dbt-lint target/manifest.json
```

## Usage

```
dbt-lint <manifest.json> [OPTIONS]

Options:
  --config PATH           Path to dbt_linter.yml config file
  --format [text|json]    Output format (default: text)
  --select TEXT           Only run these rule IDs (repeatable)
  --exclude TEXT          Skip these rule IDs (repeatable)
  --fail-on [warn|error]  Minimum severity for exit code 1 (default: warn)
```

Exit codes: `0` clean, `1` violations found, `2` tool error.

### GitHub Actions

When `GITHUB_ACTIONS=true` is set, dbt-linter emits `::error`/`::warning` workflow commands that appear as inline annotations on PR diffs.

```yaml
- name: Lint dbt project
  run: dbt-lint target/manifest.json --config dbt_linter.yml
```

## Rules

32 rules across 6 departments.

### Modeling (13)

| Rule | Description |
|---|---|
| `modeling/direct-join-to-source` | Models referencing both source and model parents |
| `modeling/downstream-depends-on-source` | Intermediate/marts models depending directly on sources |
| `modeling/staging-depends-on-staging` | Staging models depending on other staging models |
| `modeling/staging-depends-on-downstream` | Staging models depending on intermediate/marts |
| `modeling/root-models` | Models with zero parents |
| `modeling/hard-coded-references` | Hard-coded table references in SQL |
| `modeling/duplicate-sources` | Multiple source entries for the same table |
| `modeling/unused-sources` | Sources with no downstream consumers |
| `modeling/multiple-sources-joined` | Models joining more than one source directly |
| `modeling/source-fanout` | Sources with multiple direct child models |
| `modeling/model-fanout` | Models exceeding the fanout threshold |
| `modeling/too-many-joins` | Models with too many direct parents |
| `modeling/rejoining-upstream-concepts` | Models that rejoin a previously consumed concept |

### Testing (3)

| Rule | Description |
|---|---|
| `testing/missing-primary-key-tests` | Models without primary key uniqueness/not-null tests |
| `testing/sources-without-freshness` | Sources without freshness checks |
| `testing/test-coverage` | Test coverage below target by model type |

### Documentation (4)

| Rule | Description |
|---|---|
| `documentation/undocumented-models` | Models without a description |
| `documentation/undocumented-sources` | Sources without a source-level description |
| `documentation/undocumented-source-tables` | Source tables without a table-level description |
| `documentation/documentation-coverage` | Documentation coverage below target by model type |

### Structure (7)

| Rule | Description |
|---|---|
| `structure/model-naming-conventions` | Model name doesn't match prefix for its type |
| `structure/model-directories` | Model not in expected directory for its type |
| `structure/source-directories` | Source YAML not in staging directory |
| `structure/test-directories` | Test YAML in different directory than model |
| `structure/staging-materialization` | Staging model with disallowed materialization |
| `structure/intermediate-materialization` | Intermediate model with disallowed materialization |
| `structure/marts-materialization` | Marts model with disallowed materialization |

### Performance (2)

| Rule | Description |
|---|---|
| `performance/chained-views` | View chains exceeding depth threshold |
| `performance/exposure-parent-materializations` | Exposures with view/ephemeral/source parents |

### Governance (3)

| Rule | Description |
|---|---|
| `governance/public-models-without-contract` | Public models without contract enforcement |
| `governance/undocumented-public-models` | Public models missing description or column docs |
| `governance/exposures-depend-on-private-models` | Exposures depending on non-public models |

## Configuration

Create `dbt_linter.yml` to override defaults. All settings are optional.

```yaml
# Thresholds
documentation_coverage_target: 100
test_coverage_target: 100
models_fanout_threshold: 3
too_many_joins_threshold: 7
chained_views_threshold: 5

# Naming prefixes per model type
staging_prefixes: [stg_]
intermediate_prefixes: [int_]
marts_prefixes: [fct_, dim_]
base_prefixes: [base_]

# Expected directory names
staging_folder_name: staging
intermediate_folder_name: intermediate
marts_folder_name: marts

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

### Per-resource skip

Opt out of specific rules on individual resources via `meta`:

```yaml
# models/staging/stg_legacy_orders.yml
models:
  - name: stg_legacy_orders
    meta:
      dbt-linter:
        skip:
          - modeling/hard-coded-references
          - structure/model-naming-conventions
```

## Scope

dbt-linter analyzes the compiled manifest. It does not lint SQL syntax or YAML formatting. For those layers:

- SQL: [sqlfluff](https://sqlfluff.com/)
- YAML: [yamllint](https://yamllint.readthedocs.io/)

See [docs/recommended-configs.md](docs/recommended-configs.md) for suggested companion configurations.

## Requirements

- Python 3.11+
- dbt manifest v11+ (dbt 1.6+)
