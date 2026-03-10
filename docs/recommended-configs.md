# Recommended companion configs

dbt-linter handles manifest-level semantic analysis: DAG structure, naming conventions, test coverage, documentation, governance. It does not lint SQL or YAML syntax. Use these tools for the layers dbt-linter delegates.

## sqlfluff

SQL formatting and quality. Install: `pip install sqlfluff sqlfluff-templater-dbt`

`.sqlfluff`:

```ini
[sqlfluff]
templater = dbt
dialect = bigquery
# Adjust dialect to match your warehouse

[sqlfluff:rules]
# Consistent capitalisation
capitalisation_policy = lower
extended_capitalisation_policy = lower

[sqlfluff:indentation]
indent_unit = space
tab_space_size = 4

[sqlfluff:rules:convention.terminator]
# Require trailing semicolons
multiline_newline = true

[sqlfluff:rules:aliasing.table]
# Require explicit aliases
aliasing = explicit

[sqlfluff:rules:aliasing.column]
aliasing = explicit

[sqlfluff:rules:aliasing.length]
min_alias_length = 3

[sqlfluff:rules:convention.select_trailing_comma]
# Trailing commas in SELECT lists
select_clause_trailing_comma = require
```

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

Run all three together in CI:

```yaml
- name: Lint YAML
  run: yamllint models/

- name: Lint SQL
  run: sqlfluff lint models/

- name: Lint dbt project
  run: dbt-lint target/manifest.json --config dbt_linter.yml
```

sqlfluff and yamllint operate on source files. dbt-linter operates on the compiled manifest. The three tools have no overlapping rules.
