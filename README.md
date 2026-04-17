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
dbt-lint [OPTIONS] [MANIFEST]

Options:
  --config PATH           Path to dbt_lint.yml config file
  --format FORMAT         Output format: text, concise, grouped, json (default: text)
  --select TEXT           Only run these rule IDs (repeatable)
  --exclude TEXT          Skip these rule IDs (repeatable)
  --fail-on [warn|error]  Minimum severity that causes exit code 1 (default: warn)
  --fail-fast             Stop after the first violation
  --baseline PATH         Path to baseline suppressions file
  --list-rules            List all available rules and exit
  --generate-baseline     Output YAML config that suppresses all current violations
  --output PATH           Write output to file (use with --generate-baseline)
  --help                  Show help message and exit
```

Exit codes: `0` clean, `1` violations found, `2` tool error.

Color is applied to severity tags and headers when stdout is a TTY. Set `NO_COLOR=1` to disable.

### GitHub Actions

In GitHub Actions runs, dbt-lint automatically emits inline annotations on PR diffs.

```yaml
- name: Lint dbt project
  run: dbt-lint target/manifest.json --config dbt_lint.yml
```

## Rules

45 built-in rules across 6 categories: modeling, testing, documentation, structure, performance, and governance. Run `dbt-lint --list-rules` to see all rules with descriptions, or see [docs/rules.md](docs/rules.md) for the full reference.

## Configuration

Create `dbt_lint.yml` to override defaults. All settings are optional.

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

See [docs/configuration.md](docs/configuration.md) for the full reference, including naming prefixes, materialization constraints, per-resource skips, and baseline suppressions.

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
