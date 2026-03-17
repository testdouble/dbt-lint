# dbt-linter

Manifest-only semantic linter for dbt projects. Analyzes `manifest.json` to enforce DAG structure, naming conventions, test coverage, documentation standards, and governance rules. No dbt runtime or warehouse connection required.

## Contents

- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Usage](#usage)
- [Rules](#rules)
- [Configuration](#configuration)
- [Custom rules](#custom-rules)
- [Development](#development)
- [Scope](#scope)

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Python 3.11+
- dbt manifest v11+ (dbt 1.6+)

## Quick start

```bash
# Install uv
brew install uv

# Add to a dbt project
uv add git+https://github.com/yourusername/dbt-linter.git

# Or install from a local clone
uv pip install -e .

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
  --fail-fast             Stop after the first violation
  --baseline PATH         Path to dbt-lint-baseline.yml suppressions file
  --list-rules            List all available rules and exit
  --generate-baseline     Output YAML config that suppresses all current violations
  --output PATH           Write output to file (use with --generate-baseline)
```

Exit codes: `0` clean, `1` violations found, `2` tool error.

### GitHub Actions

In GitHub Actions runs, dbt-linter automatically emits inline annotations on PR diffs.

```yaml
- name: Lint dbt project
  run: dbt-lint target/manifest.json --config dbt_linter.yml
```

## Rules

41 built-in rules across 6 categories.

### Modeling (16)

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
| `modeling/staging-model-too-many-parents` | Staging models with more than one parent |
| `modeling/intermediate-fanout` | Intermediate models with too many direct dependents |
| `modeling/rejoining-upstream-concepts` | Models that rejoin a previously consumed concept |
| `modeling/duplicate-mart-concepts` | Same entity modeled in multiple mart subdirectories |

### Testing (4)

| Rule | Description |
|---|---|
| `testing/missing-primary-key-tests` | Models without primary key uniqueness/not-null tests |
| `testing/missing-relationship-tests` | Models with model parents but no relationship tests |
| `testing/sources-without-freshness` | Sources without freshness checks |
| `testing/test-coverage` | Test coverage below target by model type |

### Documentation (5)

| Rule | Description |
|---|---|
| `documentation/undocumented-models` | Models without a description |
| `documentation/undocumented-sources` | Sources without a source-level description |
| `documentation/undocumented-source-tables` | Source tables without a table-level description |
| `documentation/documentation-coverage` | Documentation coverage below target by model type |
| `documentation/column-documentation-coverage` | Column documentation coverage below target (disabled by default) |

### Structure (11)

| Rule | Description |
|---|---|
| `structure/model-name-format` | Model name is not valid snake_case |
| `structure/model-naming-conventions` | Model name doesn't match prefix for its type |
| `structure/column-naming-conventions` | Column name violates naming conventions (disabled by default) |
| `structure/model-directories` | Model not in expected directory for its type |
| `structure/source-directories` | Source YAML not in staging directory |
| `structure/test-directories` | Test YAML in different directory than model |
| `structure/staging-naming-convention` | Staging model doesn't follow `stg_<source>__<entity>` pattern |
| `structure/yaml-file-naming` | YAML schema file doesn't follow `_<dir>__<type>.yml` convention |
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
too_many_joins_threshold: 5
chained_views_threshold: 5

# Naming prefixes per model type
staging_prefixes: [stg_]
intermediate_prefixes: [int_]
base_prefixes: [base_]

# Marts use plain entity names by default (customers.sql, orders.sql).
# Uncomment to enforce fct_/dim_ prefixes:
# marts_prefixes: [fct_, dim_]

# Expected directory names (string or list)
staging_folder_name: staging
intermediate_folder_name: intermediate
marts_folder_name: marts
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

## Custom rules

Write project-specific rules in Python using the same `@rule` decorator as built-in rules. Custom rules are loaded dynamically via the `source:` directive in config.

### Writing a rule

Import from `dbt_linter.extend` and decorate a function with `@rule`:

```python
# custom_rules/avoid_select_distinct.py
import re
from dbt_linter.extend import Resource, RuleConfig, Violation, rule

_SELECT_DISTINCT = re.compile(r"\bSELECT\s+DISTINCT\b", re.IGNORECASE | re.DOTALL)

@rule(
    id="custom/avoid-select-distinct",
    description="Model uses SELECT DISTINCT instead of GROUP BY.",
    # Optional: structured metadata shown in --list-rules output
    rationale="SELECT DISTINCT is a code smell; prefer GROUP BY or QUALIFY.",
    remediation="Replace SELECT DISTINCT with GROUP BY or QUALIFY ROW_NUMBER().",
)
def avoid_select_distinct(resource: Resource, config: RuleConfig) -> Violation | None:
    if resource.resource_type != "model":
        return None
    if _SELECT_DISTINCT.search(resource.raw_code or ""):
        return Violation.from_resource(resource, f"{resource.resource_name}: uses SELECT DISTINCT")
    return None
```

Two signatures are supported:

- Per-resource: `(resource: Resource, config: RuleConfig) -> Violation | None`
- Aggregate: `(resources: list[Resource], relationships: list[Relationship], config: RuleConfig) -> list[Violation]`

The decorator validates the signature at import time.

### Public API (`dbt_linter.extend`)

| Export | Purpose |
|---|---|
| `Resource` | Frozen dataclass with 24 fields (resource_id, resource_name, resource_type, raw_code, meta, config, tags, columns, ...) |
| `Relationship` | Dependency edge between resources (parent, child, distance, ...) |
| `Violation` | Rule violation. Use `Violation.from_resource(resource, message)` to create. |
| `RuleConfig` | Per-rule config (enabled, severity, params dict) |
| `ColumnInfo` | Column metadata (name, data_type, is_described) |
| `rule` | Decorator: `@rule(id, description, *, rationale, remediation, exceptions, examples)`. Only `id` and `description` are required; the rest are optional and populate `--list-rules` output. |
| `direct_edges` | Filter relationships to distance=1 |
| `filter_by_model_type` | Filter resources by model type |
| `group_by` | Group items by key function |

### Registering in config

Add a `source:` key to the rule entry in `dbt_linter.yml`. The path is relative to the config file.

```yaml
rules:
  custom/avoid-select-distinct:
    source: custom_rules/avoid_select_distinct.py
    severity: warn
  custom/source-missing-index-meta:
    source: custom_rules/source_missing_index_meta.py
    severity: error
```

Custom rule IDs must not collide with built-in rule IDs. Multiple rules can live in the same file; the loader matches by `id`.

### Examples

See `examples/` for 5 working custom rules with tests, covering `raw_code` regex, `meta` dict checks, `config` dict access, `tags` validation, and `materialization` scoping.

## Development

```bash
# Install dependencies
uv sync

# Lint
uv run ruff check .              # check
uv run ruff check --fix .        # auto-fix

# Format
uv run ruff format --check .     # check
uv run ruff format .             # apply

# Type check
uv run ty check .

# Test
uv run pytest
```

## Scope

dbt-linter analyzes the compiled manifest. It does not lint SQL syntax or YAML formatting. For those layers:

- SQL: [sqlfluff](https://sqlfluff.com/)
- YAML: [yamllint](https://yamllint.readthedocs.io/)

See [docs/recommended-configs.md](docs/recommended-configs.md) for suggested companion configurations.
