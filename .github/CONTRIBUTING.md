# Contributing

Thanks for your interest in contributing to dbt-lint.

## Getting started

1. Clone the repository
2. Install [uv](https://docs.astral.sh/uv/) if you don't have it
3. Install dependencies: `uv sync`
4. Run tests: `uv run pytest`

## Development workflow

```bash
# Lint
uv run ruff check .

# Format
uv run ruff format --check .

# Type check
uv run ty check .

# Test
uv run pytest .
```

All four checks must pass before submitting a PR.

## Reporting issues

Open an issue on GitHub with:

- Steps to reproduce
- Expected vs actual behavior
- dbt-lint version and Python version

For security vulnerabilities, see [SECURITY.md](./SECURITY.md).

## Pull requests

- Keep PRs focused on a single logical change
- Include tests for new functionality and bug fixes
- Follow [Conventional Commits](https://www.conventionalcommits.org/) for commit messages (e.g., `feat(rules): add new governance rule`)

## Adding a built-in rule

Built-in rules use the same `@rule` decorator and signatures as custom rules. See [docs/custom-rules.md](../docs/custom-rules.md) for the API: minimal example, `RuleContext`, helpers, and signature variants. The notes below cover what's specific to in-tree rules.

### Project layout

```shell
src/dbt_lint/rules/
  modeling/
    layer_boundaries.py    # cross-layer dependency checks
    dependency_hygiene.py  # source/reference quality
    graph_structure.py     # fanout, joins, DAG shape
  structure/
    naming.py              # model/column naming conventions
    materialization.py     # allowed materializations per layer
  testing.py
  documentation.py
  performance.py
  governance.py
```

Single-module categories (`governance.py`) are auto-discovered. Sub-packaged categories (`modeling/`, `structure/`) require re-export from their `__init__.py`:

```python
# rules/modeling/__init__.py
from dbt_lint.rules.modeling.graph_structure import your_new_rule

__all__ = [
    # existing exports...
    "your_new_rule",
]
```

### Tests

Add tests in the matching file under `tests/test_rules/`. Use the `make_resource` and `default_context` fixtures:

```python
class TestYourNewRule:
    def test_flags_violation(self, make_resource, default_context):
        resource = make_resource(resource_type="model")

        violation = your_new_rule(resource, default_context)

        assert violation is not None
        assert "expected message fragment" in violation.message

    def test_clean_resource(self, make_resource, default_context):
        resource = make_resource(resource_type="model")

        assert your_new_rule(resource, default_context) is None
```

Cover positive and negative cases plus edge cases (wrong resource types, threshold boundaries, config-dependent behavior).

### Documentation

Add your rule to the appropriate table in [docs/rules.md](../docs/rules.md).
