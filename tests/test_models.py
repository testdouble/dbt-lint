"""Tests for Resource, Relationship, Violation, and DirectEdge dataclasses."""

from dbt_linter.models import DirectEdge, Relationship, Resource, Violation


class TestResource:
    def test_construction(self):
        r = Resource(
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
        )
        assert r.resource_id == "model.pkg.stg_orders"
        assert r.resource_type == "model"
        assert r.model_type == "staging"
        assert r.tags == ("daily",)
        assert r.skip_rules == frozenset()

    def test_frozen(self):
        r = Resource(
            resource_id="model.pkg.m",
            resource_name="m",
            resource_type="model",
            file_path="models/m.sql",
            model_type="other",
            materialization="table",
            schema_name="public",
            database="db",
            is_described=False,
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
        )
        try:
            r.resource_id = "other"  # type: ignore[misc]
            raise AssertionError("Should not allow mutation")
        except AttributeError:
            pass

    def test_skip_rules(self):
        r = Resource(
            resource_id="model.pkg.m",
            resource_name="m",
            resource_type="model",
            file_path="models/m.sql",
            model_type="other",
            materialization="table",
            schema_name="public",
            database="db",
            is_described=False,
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
            meta={"dbt-linter": {"skip": ["modeling/hard-coded-references"]}},
            skip_rules=frozenset(["modeling/hard-coded-references"]),
        )
        assert "modeling/hard-coded-references" in r.skip_rules
        assert "modeling/root-models" not in r.skip_rules

    def test_non_model_defaults(self):
        """Sources and exposures use empty strings for model-only fields."""
        r = Resource(
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
        )
        assert r.model_type == ""
        assert r.materialization == ""
        assert r.is_freshness_enabled is True


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
        try:
            rel.distance = 3  # type: ignore[misc]
            raise AssertionError("Should not allow mutation")
        except AttributeError:
            pass


class TestViolation:
    def test_construction(self):
        v = Violation(
            rule_id="modeling/root-models",
            resource_id="model.pkg.orphan",
            resource_name="orphan",
            message="orphan: model has no parents",
            severity="warn",
            file_path="models/orphan.sql",
        )
        assert v.rule_id == "modeling/root-models"
        assert v.severity == "warn"
        assert v.file_path == "models/orphan.sql"


class TestDirectEdge:
    def test_construction(self):
        e = DirectEdge(parent="source.pkg.raw.orders", child="model.pkg.stg_orders")
        assert e.parent == "source.pkg.raw.orders"
        assert e.child == "model.pkg.stg_orders"
