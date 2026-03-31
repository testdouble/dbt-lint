"""Shared fixtures for example custom rule tests."""

import sys
from pathlib import Path
from typing import Any

import pytest

# Add examples/ to sys.path so test files can import rule modules directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dbt_linter.config import DEFAULTS, RuleConfig
from dbt_linter.models import Relationship, Resource


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


@pytest.fixture
def default_config():
    """Default RuleConfig with all params."""
    return RuleConfig(
        enabled=True,
        severity="warn",
        exclude_resources=[],
        params=DEFAULTS,
    )
