# dbt-lint

> Static analysis tool for dbt projects.

## Overview

Checks your compiled `manifest.json` against [dbt best practices](https://docs.getdbt.com/best-practices) for DAG structure, naming conventions, test coverage, documentation standards, and governance rules. No dbt runtime or warehouse connection required.

## Contents

- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Usage](#usage)
- [Rules](#rules)
- [Configuration](#configuration)
- [Custom rules](#custom-rules)
- [Development](#development)
- [Scope](#scope)
- [License](#license)

## Prerequisites

- Python 3.11+
- dbt manifest v11+ (dbt 1.6+)

## Quick start

```bash
pip install dbt-lint
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uvx dbt-lint target/manifest.json  # try without installing
uv add --dev dbt-lint               # add as dev dependency
```

A compiled manifest is required. If you don't have one yet:

```bash
dbt parse  # or dbt compile / dbt run
```

## Usage

```shell
dbt-lint check [OPTIONS] MANIFEST
dbt-lint rule [OPTIONS] [RULE_ID]

check options:
  --config PATH                          Path to dbt-lint.yml config file
  --output-format [text|concise|grouped|json]
                                         Output format (default: text)
  --select TEXT                          Only run these rule IDs (repeatable)
  --exclude TEXT                         Skip these rule IDs (repeatable)
  --fail-on [warn|error]                 Minimum severity for exit code 1 (default: warn)
  --fail-fast                            Stop after the first violation
  --exit-zero                            Force exit 0 regardless of violations
  --isolated                             Bypass config discovery and suppressions auto-load
  --suppressions PATH                    Path to suppressions file
  --write-suppressions                   Emit a suppressions YAML to stdout

rule options:
  --all                                  List every rule
  --config PATH                          Path to dbt-lint.yml config file
  --isolated                             Bypass config discovery (skips custom-rule loading)
  --output-format [text|json]            Output format (default: text)
```

Exit codes: `0` clean, `1` violations found, `2` tool error.

Config auto-discovery walks up from cwd looking first for `pyproject.toml [tool.dbt-lint]`, then for `dbt-lint.yml`. Pass `--config PATH` to override or `--isolated` to skip discovery entirely.

When a `.dbt-lint-suppressions.yml` sits next to the discovered config or in the cwd, `check` applies it automatically. `--suppressions PATH` overrides the location; `--isolated` and `--write-suppressions` skip the auto-load.

Color is applied to severity tags and headers when stdout is a TTY. Set `NO_COLOR=1` to disable.

### GitHub Actions

In GitHub Actions runs, dbt-lint automatically emits inline annotations on PR diffs.

```yaml
- name: Lint dbt project
  run: dbt-lint check target/manifest.json --config dbt-lint.yml
```

## Rules

Built-in rules cover modeling, testing, documentation, structure, performance, and governance. Run `dbt-lint rule --all` to see all rules with descriptions, or see [docs/rules.md](docs/rules.md) for the full reference.

## Configuration

Create `dbt-lint.yml` to override defaults, or add a `[tool.dbt-lint]` section to `pyproject.toml`. All settings are optional.

```yaml
# Adjust thresholds
models_fanout_threshold: 3
test_coverage_target: 100

# Override rule severity or disable rules
rules:
  modeling/too-many-joins:
    severity: error
  structure/intermediate-materialization:
    enabled: false
```

See [docs/configuration.md](docs/configuration.md) for the full reference, including naming prefixes, materialization constraints, per-resource skips, and suppressions.

## Custom rules

Write project-specific rules in Python using the `@rule` decorator and the `dbt_lint.extend` public API. See [docs/custom-rules.md](docs/custom-rules.md) for the full guide, API reference, and examples.

## Development

See [CONTRIBUTING.md](.github/CONTRIBUTING.md) for setup, workflow, and how to add rules.

## Scope

dbt-lint checks your project against the [dbt best practices](https://docs.getdbt.com/best-practices) that can be detected in the compiled manifest. SQL style and YAML formatting fall outside the manifest and are covered by companion tools:

- SQL: [SQLFluff](https://sqlfluff.com/)
- YAML: [yamllint](https://yamllint.readthedocs.io/)

See [docs/companion-tools.md](docs/companion-tools.md) for ready-to-use configs that align with dbt best practices.

## License

[MIT](./LICENSE) — see LICENSE file for details.
