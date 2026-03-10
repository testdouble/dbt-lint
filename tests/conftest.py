"""Shared test fixtures: Resource and Relationship builders."""

import pytest

from dbt_linter.models import Relationship, Resource


@pytest.fixture
def make_resource():
    """Factory fixture for Resource with sensible defaults."""

    _counter = 0

    def _make(**overrides) -> Resource:
        nonlocal _counter
        _counter += 1
        defaults = {
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
            "is_primary_key_tested": True,
            "tags": (),
            "meta": {},
            "skip_rules": frozenset(),
        }
        defaults.update(overrides)
        return Resource(**defaults)

    return _make


@pytest.fixture
def make_relationship():
    """Factory fixture for Relationship with sensible defaults."""

    _counter = 0

    def _make(**overrides) -> Relationship:
        nonlocal _counter
        _counter += 1
        defaults = {
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
