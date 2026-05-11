"""Shared test fixtures: Resource, Relationship, Violation, and RuleDef builders."""

from typing import Any

import pytest

from dbt_lint.models import Relationship, Resource, Violation
from dbt_lint.rules import RuleDef


def _noop_rule_fn(*_args: Any, **_kwargs: Any) -> None:
    """Placeholder rule body for tests that never invoke the rule function."""


@pytest.fixture
def make_rule():
    """Factory fixture for RuleDef with sensible defaults.

    The default ``fn`` is a no-op; pass an explicit callable when the test
    invokes the rule (e.g., engine dispatch tests).
    """

    def _make(
        rule_id: str = "test/stub",
        *,
        fn: Any = _noop_rule_fn,
        is_per_resource: bool = True,
    ) -> RuleDef:
        return RuleDef(
            id=rule_id,
            category=rule_id.split("/", maxsplit=1)[0],
            description=f"stub {rule_id}",
            fn=fn,
            is_per_resource=is_per_resource,
        )

    return _make


@pytest.fixture
def make_resource():
    """Factory fixture for Resource with sensible defaults."""

    _counter = 0

    def _make(**overrides: Any) -> Resource:
        nonlocal _counter
        _counter += 1
        defaults: dict[str, Any] = {
            "resource_id": f"model.pkg.model_{_counter}",
            "resource_name": f"model_{_counter}",
            "resource_type": "model",
            "file_path": f"models/model_{_counter}.sql",
            "model_type": "other",
            "materialization": "table",
            "schema_name": "public",
            "database": "analytics",
            "is_described": True,
            "is_public": False,
            "is_contract_enforced": False,
            "hard_coded_references": False,
            "number_of_columns": 5,
            "number_of_documented_columns": 5,
            "is_freshness_enabled": False,
            "number_of_tests": 1,
            "is_primary_key_tested": True,
            "has_relationship_tests": False,
            "patch_path": "",
            "tags": (),
            "meta": {},
            "skip_rules": frozenset(),
            "raw_code": "",
            "config": {},
            "columns": (),
        }
        defaults.update(overrides)
        return Resource(**defaults)

    return _make


@pytest.fixture
def make_violation():
    """Factory fixture for Violation with sensible defaults.

    When resource_id is overridden but resource_name is not,
    resource_name is derived from the last segment of resource_id.
    """

    def _make(**overrides: Any) -> Violation:
        defaults: dict[str, Any] = {
            "rule_id": "documentation/undocumented-models",
            "resource_id": "model.pkg.stg_users",
            "resource_name": "stg_users",
            "message": "stg_users: missing description",
            "severity": "warn",
            "file_path": "models/staging/stg_users.sql",
            "patch_path": "",
        }
        if "resource_id" in overrides and "resource_name" not in overrides:
            defaults["resource_name"] = overrides["resource_id"].split(".")[-1]
        defaults.update(overrides)
        return Violation(**defaults)

    return _make


@pytest.fixture
def make_relationship():
    """Factory fixture for Relationship with sensible defaults."""

    _counter = 0

    def _make(**overrides: Any) -> Relationship:
        nonlocal _counter
        _counter += 1
        defaults: dict[str, Any] = {
            "parent": f"model.pkg.parent_{_counter}",
            "child": f"model.pkg.child_{_counter}",
            "parent_resource_type": "model",
            "child_resource_type": "model",
            "parent_model_type": "staging",
            "child_model_type": "marts",
            "parent_materialization": "view",
            "parent_is_public": False,
            "distance": 1,
            "is_dependent_on_chain_of_views": False,
        }
        defaults.update(overrides)
        return Relationship(**defaults)

    return _make
