# Recommended companion configs

dbt-linter handles manifest-level semantic analysis: DAG structure, naming conventions, test coverage, documentation, governance. It does not lint SQL syntax or YAML formatting. The tools below cover those layers.

Each tool owns a distinct scope with no overlap.

| Concern | Tool |
|---|---|
| DAG structure, layer boundaries | dbt-linter |
| Model/column naming conventions | dbt-linter |
| Test coverage, primary keys, freshness | dbt-linter |
| Documentation coverage | dbt-linter |
| Governance, contracts | dbt-linter |
| Project-specific patterns | dbt-linter (custom rules) |
| SQL formatting and style | SQLFluff |
| SQL quality (SELECT *, subqueries, dead CTEs) | SQLFluff |
| YAML formatting | yamllint |

## SQLFluff

SQL formatting and quality. Install: `pip install sqlfluff sqlfluff-templater-dbt`

The config below follows dbt best practices defaults. Adjust dialect, capitalization, and line length to match your project conventions, then use dbt-linter's config overrides for any structural deviations.

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

# --- SQL quality (patterns dbt-linter delegates to SQLFluff) ---

# AM04: No SELECT * (expand column lists explicitly)
# Enabled by default, no config needed.

# ST06: Prefer CTEs over subqueries
# Enabled by default, no config needed.

# LT12: Unused CTEs (dead code that should be removed)
# Enabled by default, no config needed.
```

### Patterns requiring manual review

These are not reliably enforceable via config. Document them in PR review guidelines or `CONTRIBUTING.md`:

- No right joins (restructure to select from the correct table)
- GROUP BY column index, not column name
- Fields before aggregates/window functions in SELECT
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
  run: dbt-lint target/manifest.json --config dbt_linter.yml
```

sqlfluff and yamllint operate on source files. dbt-linter operates on the compiled manifest.
