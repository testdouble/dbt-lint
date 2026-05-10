# Companion tool configs

Two areas of the [dbt best practices](https://docs.getdbt.com/best-practices) fall outside what dbt-lint can detect in the manifest: SQL style and YAML formatting. The configs below cover those gaps.

| Layer | Tool | Operates on |
| --- | --- | --- |
| DAG, naming, testing, docs, governance | dbt-lint | `manifest.json` |
| SQL formatting and quality | SQLFluff | `.sql` source files |
| YAML formatting | yamllint | `.yml` source files |

## SQLFluff

SQL formatting and quality. Install: `pip install sqlfluff sqlfluff-templater-dbt`

The config below aligns with dbt best practices defaults. Adjust dialect, capitalization, and line length to match your project.

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
```

### Alternative: sqlfmt

[sqlfmt](https://github.com/tconbeer/sqlfmt) is a more opinionated, less configurable SQL formatter. If your team prefers convention over configuration, sqlfmt can replace SQLFluff's formatting rules. You would still use SQLFluff for quality rules (AM04, ST06, LT12, join/union checks).

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

All three tools run independently with no overlapping rules. SQLFluff and yamllint operate on source files; dbt-lint operates on the compiled manifest.

```yaml
steps:
  - name: Lint YAML
    run: yamllint models/

  - name: Lint SQL
    run: sqlfluff lint models/

  - name: Lint dbt project
    run: dbt-lint check target/manifest.json --config dbt-lint.yml
```
