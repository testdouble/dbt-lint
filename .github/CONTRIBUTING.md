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

Categories that are single modules (`governance.py`) are discovered automatically. Sub-packaged categories (`modeling/`, `structure/`) require re-export from their `__init__.py`.

### 1. Write the rule

Add your function in the appropriate module. Use the `@rule` decorator with at minimum `id` and `description`:

```python
from dbt_lint.config import RuleConfig
from dbt_lint.models import Resource, Violation
from dbt_lint.rules import rule


@rule(
    id="category/rule-name",
    description="Short description shown in rule tables.",
    rationale="Why this matters. Shown in --list-rules output.",
    remediation="How to fix it.",
)
def rule_name(resource: Resource, config: RuleConfig) -> Violation | None:
    if resource.resource_type != "model":
        return None
    # Check logic here
    return Violation.from_resource(resource, f"{resource.resource_name}: explanation")
```

Two function signatures are supported:

- Per-resource: `(resource: Resource, config: RuleConfig) -> Violation | None`
- Aggregate: `(resources: list[Resource], relationships: list[Relationship], config: RuleConfig) -> list[Violation]`

Use aggregate when the rule needs to compare across resources or traverse the DAG. The decorator validates the signature at import time.

Helpers available from `dbt_lint.rules`:

| Helper | Purpose |
| --- | --- |
| `direct_edges(relationships)` | Filter to distance=1 edges |
| `resources_by_id(resources)` | Index resources by ID |
| `resolve_name(by_id, resource_id)` | Human-readable name for a resource ID |
| `group_by(items, key)` | Group items by key function |
| `filter_by_model_type(resources, type)` | Filter resources to a model type |

### 2. Register the rule

For sub-packaged categories, re-export your function from the category's `__init__.py`:

```python
# rules/modeling/__init__.py
from dbt_lint.rules.modeling.graph_structure import your_new_rule

__all__ = [
    # ... existing exports ...
    "your_new_rule",
]
```

For single-module categories (`testing.py`, `governance.py`, etc.), no extra registration is needed.

### 3. Write tests

Add tests in the matching file under `tests/test_rules/`. Use the `make_resource` and `default_config` fixtures:

```python
class TestYourNewRule:
    def test_flags_violation(self, make_resource, default_config):
        resource = make_resource(
            resource_type="model",
            # set fields relevant to your rule
        )

        violation = your_new_rule(resource, default_config)

        assert violation is not None
        assert "expected message fragment" in violation.message

    def test_clean_resource(self, make_resource, default_config):
        resource = make_resource(
            resource_type="model",
            # set fields for a passing case
        )

        assert your_new_rule(resource, default_config) is None
```

Test both positive (violation) and negative (clean) cases. Cover edge cases like wrong resource types, boundary values for thresholds, and any config-dependent behavior.

### 4. Update the docs

Add your rule to the appropriate table in [docs/rules.md](../docs/rules.md) and update the rule count in both `docs/rules.md` and `README.md`.
