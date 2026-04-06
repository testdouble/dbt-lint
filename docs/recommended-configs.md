# Recommended companion configs

dbt-lint handles manifest-level semantic analysis: DAG structure, naming conventions, test coverage, documentation, governance. It does not lint SQL syntax or YAML formatting. The tools below cover those layers.

Each tool owns a distinct scope with no overlap.

| Concern | Tool |
|---|---|
| DAG structure, layer boundaries | dbt-lint |
| Model/column naming conventions | dbt-lint |
| Test coverage, primary keys, freshness | dbt-lint |
| Documentation coverage | dbt-lint |
| Governance, contracts | dbt-lint |
| Project-specific patterns | dbt-lint (custom rules) |
| SQL formatting and style | SQLFluff (or sqlfmt) |
| SQL quality (SELECT *, subqueries, dead CTEs) | SQLFluff |
| YAML formatting | yamllint |
| Pre-commit enforcement | dbt-checkpoint |
| Build performance monitoring | dbt_artifacts / dbt Cloud |
| Semantic layer, NRT patterns, grants | Manual review / dbt parse |

## SQLFluff

SQL formatting and quality. Install: `pip install sqlfluff sqlfluff-templater-dbt`

The config below follows dbt best practices defaults. Adjust dialect, capitalization, and line length to match your project conventions, then use dbt-lint's config overrides for any structural deviations.

`.sqlfluff`:

```ini
[sqlfluff]
templater = dbt
dialect = bigquery
# Adjust dialect to match your warehouse (postgres, snowflake, etc.)

# --- Capitalization ---

[sqlfluff:rules]
# dbt best practices: lowercase keywords and function names (CP01, CP02)
capitalisation_policy = lower
extended_capitalisation_policy = lower

# --- Indentation ---

[sqlfluff:indentation]
indent_unit = space
tab_space_size = 4

# --- Formatting ---

[sqlfluff:rules:layout.long_lines]
# dbt best practices: max 80 characters
max_line_length = 80

[sqlfluff:rules:convention.select_trailing_comma]
select_clause_trailing_comma = require

[sqlfluff:rules:convention.terminator]
multiline_newline = true

[sqlfluff:rules:jinja.padding]
# Spaces inside Jinja delimiters: {{ this }} not {{this}}

# --- Aliasing ---

[sqlfluff:rules:aliasing.table]
aliasing = explicit

[sqlfluff:rules:aliasing.column]
aliasing = explicit

[sqlfluff:rules:aliasing.length]
min_alias_length = 3

# --- Join quality ---

[sqlfluff:rules:ambiguous.join]
# Require explicit join types: INNER JOIN not JOIN
fully_qualify_join_types = both

[sqlfluff:rules:ambiguous.union]
# Prefer UNION ALL / UNION DISTINCT over bare UNION

# --- SQL quality (patterns dbt-lint delegates to SQLFluff) ---

# AM04: No SELECT * (expand column lists explicitly)
# Enabled by default, no config needed.

# ST06: Prefer CTEs over subqueries
# Enabled by default, no config needed.

# LT12: Unused CTEs (dead code that should be removed)
# Enabled by default, no config needed.
```

### Alternative: sqlfmt

[sqlfmt](https://sqlfmt.com/) is a more opinionated, less configurable SQL formatter. If your team prefers convention over configuration, sqlfmt can replace SQLFluff's formatting rules. You would still use SQLFluff for quality rules (AM04, ST06, LT12, join/union checks).

### Patterns requiring manual review

These are not reliably enforceable via config. Document them in PR review guidelines or `CONTRIBUTING.md`:

- No right joins (restructure to select from the correct table)
- GROUP BY column index, not column name
- Fields before aggregates/window functions in SELECT
- Import CTEs should limit columns and include `where` clauses, not `select *`
- Column ordering within models: IDs, strings, numerics, booleans, dates, timestamps
- No nested Jinja curlies inside `{{ }}` (except in hooks)
- No ORDER BY in non-final models (AM05 is not a built-in rule)
- Logic reimplementation (same metric calculated differently in two models)
- Commented-out SQL (LT01 catches some cases, but high false positive rate)

## yamllint

YAML structure and formatting for schema files. Install: `pip install yamllint`

`.yamllint`:

```yaml
extends: default

rules:
  line-length:
    max: 120
  indentation:
    spaces: 2
  truthy:
    # dbt uses bare true/false in YAML
    allowed-values: ["true", "false"]
  document-start: disable
  comments:
    min-spaces-from-content: 1
```

## CI integration

Run all three tools in CI. They operate on different artifacts with no overlapping rules.

```yaml
- name: Lint YAML
  run: yamllint models/

- name: Lint SQL
  run: sqlfluff lint models/

- name: Lint dbt project
  run: dbt-lint target/manifest.json --config dbt_lint.yml
```

sqlfluff and yamllint operate on source files. dbt-lint operates on the compiled manifest.

## Pre-commit hooks

[dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint) provides pre-commit hooks for dbt projects. Implement after your style guide is published and the codebase conforms. Hooks enforce standards on commit rather than in CI, catching issues earlier.

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/dbt-checkpoint/dbt-checkpoint
    rev: v2.0.3
    hooks:
      - id: check-model-has-tests
      - id: check-model-has-description
```

Note: dbt-checkpoint hooks overlap with some dbt-lint rules (test coverage, documentation). Use dbt-checkpoint for fast local feedback on commit; use dbt-lint in CI for the full manifest-level analysis that requires a compiled project.

## Build monitoring

dbt best practices recommend monitoring view build times (target: < 1-2 seconds) and identifying slow models in the DAG. These are runtime concerns outside the scope of static linting.

Options:
- dbt Cloud: Model Timing visualization in the IDE.
- dbt Core CLI: check start/completion timestamps and build duration in CLI output.
- [`dbt_artifacts`](https://github.com/brooklyn-data/dbt_artifacts) package: persist build metadata to your warehouse for custom dashboards.

## Practices outside lint scope

The following dbt best practices are architectural or domain-specific guidance that no static lint tool can enforce. They are documented here for completeness.

- **Semantic layer / MetricFlow**: entity naming (singular form), dimension/measure configuration, metric type selection. These live in YAML under `semantic_models:` and `metrics:` keys. Review manually or via dbt's built-in `dbt parse` validation.
- **Near real-time patterns**: incremental strategy selection (merge vs CDC vs microbatch), dynamic table configuration, lambda view architecture. Architectural decisions, not lintable.
- **`data_tests:` key**: dbt-core v1.8+ renamed `tests:` to `data_tests:` in YAML. Both work, but new projects should use `data_tests:`. This is a YAML convention, not a manifest-level check.
- **Unity Catalog / warehouse-specific grants**: access control matrix, catalog isolation, OIDC configuration. Infrastructure-level decisions.
