"""Tests for Resource, Relationship, Violation, and DirectEdge dataclasses."""

import pytest

from dbt_linter.models import ColumnInfo, DirectEdge, Relationship, Resource, Violation


class TestResource:
    def test_construction(self):
        resource = Resource(
            resource_id="model.pkg.stg_orders",
            resource_name="stg_orders",
            resource_type="model",
            file_path="models/staging/stg_orders.sql",
            model_type="staging",
            materialization="view",
            schema_name="staging",
            database="analytics",
            is_described=True,
            is_public=False,
            is_contract_enforced=False,
            hard_coded_references=False,
            number_of_columns=5,
            number_of_documented_columns=5,
            is_freshness_enabled=False,
            is_primary_key_tested=True,
            has_relationship_tests=False,
            patch_path="",
            tags=("daily",),
            meta={},
            skip_rules=frozenset(),
            raw_code="SELECT * FROM {{ ref('raw_orders') }}",
            config={"materialized": "view"},
            columns=(),
        )
        assert resource.resource_id == "model.pkg.stg_orders"
        assert resource.resource_type == "model"
        assert resource.model_type == "staging"
        assert resource.tags == ("daily",)
        assert resource.skip_rules == frozenset()
        assert resource.raw_code == "SELECT * FROM {{ ref('raw_orders') }}"
        assert resource.config == {"materialized": "view"}

    def test_frozen(self, make_resource):
        resource = make_resource()
        with pytest.raises(AttributeError):
            resource.resource_id = "other"

    def test_skip_rules(self, make_resource):
        resource = make_resource(
            skip_rules=frozenset(["modeling/hard-coded-references"]),
        )
        assert "modeling/hard-coded-references" in resource.skip_rules
        assert "modeling/root-models" not in resource.skip_rules

    def test_non_model_defaults(self):
        """Sources and exposures use empty strings for model-only fields."""
        resource = Resource(
            resource_id="source.pkg.raw.orders",
            resource_name="orders",
            resource_type="source",
            file_path="models/staging/sources.yml",
            model_type="",
            materialization="",
            schema_name="raw",
            database="analytics",
            is_described=True,
            is_public=False,
            is_contract_enforced=False,
            hard_coded_references=False,
            number_of_columns=0,
            number_of_documented_columns=0,
            is_freshness_enabled=True,
            is_primary_key_tested=False,
            has_relationship_tests=False,
            patch_path="",
            tags=(),
            meta={},
            skip_rules=frozenset(),
            raw_code="",
            config={},
            columns=(),
        )
        assert resource.model_type == ""
        assert resource.materialization == ""
        assert resource.is_freshness_enabled is True


class TestRelationship:
    def test_construction(self):
        rel = Relationship(
            parent="source.pkg.raw.orders",
            child="model.pkg.stg_orders",
            parent_resource_type="source",
            child_resource_type="model",
            parent_model_type="",
            child_model_type="staging",
            parent_materialization="",
            parent_is_public=False,
            distance=1,
            is_dependent_on_chain_of_views=False,
        )
        assert rel.parent == "source.pkg.raw.orders"
        assert rel.distance == 1
        assert rel.is_dependent_on_chain_of_views is False

    def test_frozen(self):
        rel = Relationship(
            parent="a",
            child="b",
            parent_resource_type="model",
            child_resource_type="model",
            parent_model_type="staging",
            child_model_type="marts",
            parent_materialization="view",
            parent_is_public=False,
            distance=2,
            is_dependent_on_chain_of_views=True,
        )
        with pytest.raises(AttributeError):
            rel.distance = 3  # type: ignore[misc]


class TestColumnInfo:
    def test_construction(self):
        column = ColumnInfo(name="order_id", data_type="integer", is_described=True)
        assert column.name == "order_id"
        assert column.data_type == "integer"
        assert column.is_described is True

    def test_frozen(self):
        column = ColumnInfo(name="id", data_type="", is_described=False)
        with pytest.raises(AttributeError):
            column.name = "other"  # type: ignore[misc]


class TestViolation:
    def test_construction(self):
        violation = Violation(
            rule_id="modeling/root-models",
            resource_id="model.pkg.orphan",
            resource_name="orphan",
            message="orphan: model has no parents",
            severity="warn",
            file_path="models/orphan.sql",
        )
        assert violation.rule_id == "modeling/root-models"
        assert violation.severity == "warn"
        assert violation.file_path == "models/orphan.sql"

    def test_from_resource(self):
        resource = Resource(
            resource_id="model.pkg.stg_orders",
            resource_name="stg_orders",
            resource_type="model",
            file_path="models/staging/stg_orders.sql",
            model_type="staging",
            materialization="view",
            schema_name="staging",
            database="analytics",
            is_described=True,
            is_public=False,
            is_contract_enforced=False,
            hard_coded_references=False,
            number_of_columns=0,
            number_of_documented_columns=0,
            is_freshness_enabled=False,
            is_primary_key_tested=False,
            has_relationship_tests=False,
            patch_path="",
            tags=(),
            meta={},
            skip_rules=frozenset(),
            raw_code="",
            config={},
            columns=(),
        )
        violation = Violation.from_resource(
            resource, "stg_orders: uses SELECT DISTINCT"
        )
        assert violation.resource_id == "model.pkg.stg_orders"
        assert violation.resource_name == "stg_orders"
        assert violation.file_path == "models/staging/stg_orders.sql"
        assert violation.message == "stg_orders: uses SELECT DISTINCT"
        assert violation.rule_id == ""
        assert violation.severity == ""


class TestDirectEdge:
    def test_construction(self):
        edge = DirectEdge(parent="source.pkg.raw.orders", child="model.pkg.stg_orders")
        assert edge.parent == "source.pkg.raw.orders"
        assert edge.child == "model.pkg.stg_orders"
