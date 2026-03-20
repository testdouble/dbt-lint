"""Tests for manifest.py: JSON manifest parsing into Resources and DirectEdges."""

from __future__ import annotations

import json

import pytest

from dbt_linter.config import DEFAULTS, load_config
from dbt_linter.manifest import (
    _classify_model_type,
    _columns_to_tuple,
    _exposure_to_resource,
    _extract_edges,
    _extract_skip_rules,
    _has_hard_coded_references,
    _model_to_resource,
    _source_to_resource,
    parse_manifest,
)
from dbt_linter.models import ColumnInfo, DirectEdge


class TestClassifyModelType:
    """Two-pass heuristic: prefix match first, then directory match."""

    def test_staging_prefix(self):
        assert (
            _classify_model_type(
                "stg_orders", "models/staging/orders/stg_orders.sql", DEFAULTS
            )
            == "staging"
        )

    def test_intermediate_prefix(self):
        assert (
            _classify_model_type(
                "int_orders_pivoted",
                "models/intermediate/int_orders_pivoted.sql",
                DEFAULTS,
            )
            == "intermediate"
        )

    def test_marts_fct_prefix(self):
        assert (
            _classify_model_type("fct_orders", "models/marts/fct_orders.sql", DEFAULTS)
            == "marts"
        )

    def test_marts_dim_prefix(self):
        assert (
            _classify_model_type(
                "dim_customers", "models/marts/dim_customers.sql", DEFAULTS
            )
            == "marts"
        )

    def test_base_prefix(self):
        assert (
            _classify_model_type("base_orders", "models/base/base_orders.sql", DEFAULTS)
            == "base"
        )

    def test_other_prefix(self):
        assert (
            _classify_model_type(
                "rpt_weekly_sales", "models/reports/rpt_weekly_sales.sql", DEFAULTS
            )
            == "other"
        )

    def test_directory_fallback_staging(self):
        """No prefix match, falls back to directory."""
        assert (
            _classify_model_type(
                "orders", "models/staging/src_name/orders.sql", DEFAULTS
            )
            == "staging"
        )

    def test_directory_fallback_marts(self):
        assert (
            _classify_model_type("orders", "models/marts/orders.sql", DEFAULTS)
            == "marts"
        )

    def test_directory_fallback_intermediate(self):
        assert (
            _classify_model_type("orders", "models/intermediate/orders.sql", DEFAULTS)
            == "intermediate"
        )

    def test_prefix_takes_precedence_over_directory(self):
        """Model named stg_ but in marts directory: prefix wins."""
        assert (
            _classify_model_type("stg_orders", "models/marts/stg_orders.sql", DEFAULTS)
            == "staging"
        )

    def test_no_match_returns_other(self):
        assert (
            _classify_model_type("my_model", "models/custom/my_model.sql", DEFAULTS)
            == "other"
        )

    def test_nested_directory_match(self):
        """Directory match works with nested paths."""
        assert (
            _classify_model_type("orders", "models/staging/stripe/orders.sql", DEFAULTS)
            == "staging"
        )


class TestHasHardCodedReferences:
    def test_clean_sql_no_references(self):
        sql = "SELECT * FROM {{ ref('stg_orders') }}"
        assert _has_hard_coded_references(sql) is False

    def test_source_macro_is_clean(self):
        sql = "SELECT * FROM {{ source('stripe', 'payments') }}"
        assert _has_hard_coded_references(sql) is False

    def test_schema_dot_table_unquoted(self):
        sql = "SELECT * FROM raw_data.orders"
        assert _has_hard_coded_references(sql) is True

    def test_schema_dot_table_after_join(self):
        sql = "JOIN analytics.customers ON o.customer_id = c.id"
        assert _has_hard_coded_references(sql) is True

    def test_database_schema_table(self):
        sql = "SELECT * FROM prod_db.raw_data.orders"
        assert _has_hard_coded_references(sql) is True

    def test_var_function_in_from(self):
        sql = "SELECT * FROM {{ var('schema') }}.orders"
        assert _has_hard_coded_references(sql) is True

    def test_quoted_schema_table(self):
        sql = 'SELECT * FROM "raw_data"."orders"'
        assert _has_hard_coded_references(sql) is True

    def test_backtick_quoted(self):
        sql = "SELECT * FROM `raw_data`.`orders`"
        assert _has_hard_coded_references(sql) is True

    def test_empty_sql(self):
        assert _has_hard_coded_references("") is False

    def test_ref_and_hardcoded_mixed(self):
        """If any hard-coded reference exists, flag it."""
        sql = """
        SELECT *
        FROM {{ ref('stg_orders') }} o
        JOIN raw_data.customers c ON o.id = c.id
        """
        assert _has_hard_coded_references(sql) is True

    def test_jinja_comment_not_flagged(self):
        """Jinja ref/source calls should not trigger."""
        sql = """
        {# This model loads from raw_data.orders #}
        SELECT * FROM {{ ref('stg_orders') }}
        """
        assert _has_hard_coded_references(sql) is False


class TestExtractSkipRules:
    def test_no_meta(self):
        assert _extract_skip_rules({}) == frozenset()

    def test_no_dbt_linter_key(self):
        assert _extract_skip_rules({"other": "value"}) == frozenset()

    def test_skip_list(self):
        meta = {
            "dbt-linter": {
                "skip": [
                    "modeling/hard-coded-references",
                    "structure/model-naming-conventions",
                ]
            }
        }
        assert _extract_skip_rules(meta) == frozenset(
            {"modeling/hard-coded-references", "structure/model-naming-conventions"}
        )

    def test_empty_skip_list(self):
        meta = {"dbt-linter": {"skip": []}}
        assert _extract_skip_rules(meta) == frozenset()

    def test_missing_skip_key(self):
        meta = {"dbt-linter": {"other": True}}
        assert _extract_skip_rules(meta) == frozenset()


class TestColumnsToTuple:
    def test_empty_dict(self):
        assert not _columns_to_tuple({})

    def test_single_column(self):
        cols = {
            "id": {
                "name": "id",
                "description": "Primary key",
                "data_type": "integer",
            }
        }
        result = _columns_to_tuple(cols)
        assert len(result) == 1
        assert result[0] == ColumnInfo(
            name="id", data_type="integer", is_described=True
        )

    def test_undescribed_column(self):
        cols = {"id": {"name": "id", "description": "", "data_type": "integer"}}
        result = _columns_to_tuple(cols)
        assert result[0].is_described is False

    def test_missing_data_type(self):
        cols = {"id": {"name": "id", "description": "PK"}}
        result = _columns_to_tuple(cols)
        assert result[0].data_type == ""

    def test_multiple_columns(self):
        cols = {
            "id": {"name": "id", "description": "PK", "data_type": "integer"},
            "amount": {"name": "amount", "description": "", "data_type": "numeric"},
        }
        result = _columns_to_tuple(cols)
        assert len(result) == 2

    def test_name_falls_back_to_key(self):
        cols = {"order_id": {"description": "PK"}}
        result = _columns_to_tuple(cols)
        assert result[0].name == "order_id"


class TestModelToResource:
    @pytest.fixture
    def model_node(self):
        return {
            "unique_id": "model.pkg.stg_orders",
            "name": "stg_orders",
            "resource_type": "model",
            "original_file_path": "models/staging/stripe/stg_orders.sql",
            "fqn": ["pkg", "staging", "stripe", "stg_orders"],
            "config": {
                "materialized": "view",
                "meta": {},
                "tags": ["daily"],
            },
            "description": "Staged orders from Stripe",
            "columns": {
                "id": {"name": "id", "description": "Primary key"},
                "amount": {"name": "amount", "description": ""},
            },
            "raw_code": "SELECT * FROM {{ source('stripe', 'orders') }}",
            "access": "protected",
            "contract": {"enforced": False},
            "schema": "staging",
            "database": "analytics",
        }

    @pytest.fixture
    def test_index(self):
        return {
            "model.pkg.stg_orders": [
                {"name": "unique", "namespace": "dbt", "kwargs": {}},
                {"name": "not_null", "namespace": "dbt", "kwargs": {}},
            ]
        }

    def test_basic_fields(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.resource_id == "model.pkg.stg_orders"
        assert resource.resource_name == "stg_orders"
        assert resource.resource_type == "model"
        assert resource.file_path == "models/staging/stripe/stg_orders.sql"
        assert resource.materialization == "view"
        assert resource.schema_name == "staging"
        assert resource.database == "analytics"

    def test_model_type_classified(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.model_type == "staging"

    def test_description_flag(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.is_described is True

    def test_undescribed_model(self, model_node, test_index):
        model_node["description"] = ""
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.is_described is False

    def test_column_counts(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.number_of_columns == 2
        assert resource.number_of_documented_columns == 1  # only "id" has description

    def test_hard_coded_references_false(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.hard_coded_references is False

    def test_hard_coded_references_true(self, model_node, test_index):
        model_node["raw_code"] = "SELECT * FROM raw.orders"
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.hard_coded_references is True

    def test_primary_key_tested(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.is_primary_key_tested is True

    def test_not_primary_key_tested(self, model_node):
        resource = _model_to_resource(model_node, {}, DEFAULTS)
        assert resource.is_primary_key_tested is False

    def test_access_public(self, model_node, test_index):
        model_node["access"] = "public"
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.is_public is True

    def test_contract_enforced(self, model_node, test_index):
        model_node["contract"]["enforced"] = True
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.is_contract_enforced is True

    def test_tags(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.tags == ("daily",)

    def test_skip_rules_from_meta(self, model_node, test_index):
        model_node["config"]["meta"] = {
            "dbt-linter": {"skip": ["modeling/hard-coded-references"]}
        }
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.skip_rules == frozenset({"modeling/hard-coded-references"})

    def test_raw_code(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.raw_code == "SELECT * FROM {{ source('stripe', 'orders') }}"

    def test_config_dict(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.config["materialized"] == "view"
        assert resource.config["tags"] == ["daily"]

    def test_columns_tuple(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert len(resource.columns) == 2
        names = {c.name for c in resource.columns}
        assert names == {"id", "amount"}

    def test_columns_described_flag(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        by_name = {c.name: c for c in resource.columns}
        assert by_name["id"].is_described is True
        assert by_name["amount"].is_described is False

    def test_empty_raw_code(self, model_node, test_index):
        model_node["raw_code"] = ""
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.raw_code == ""

    def test_missing_raw_code(self, model_node, test_index):
        del model_node["raw_code"]
        resource = _model_to_resource(model_node, test_index, DEFAULTS)
        assert resource.raw_code == ""

    def test_has_relationship_tests(self, model_node):
        test_index = {
            "model.pkg.stg_orders": [
                {"name": "relationships", "namespace": "dbt", "kwargs": {}},
            ]
        }

        resource = _model_to_resource(model_node, test_index, DEFAULTS)

        assert resource.has_relationship_tests is True

    def test_has_no_relationship_tests(self, model_node, test_index):
        resource = _model_to_resource(model_node, test_index, DEFAULTS)

        assert resource.has_relationship_tests is False


class TestSourceToResource:
    @pytest.fixture
    def source_node(self):
        return {
            "unique_id": "source.pkg.stripe.payments",
            "name": "payments",
            "source_name": "stripe",
            "source_description": "Stripe payment data",
            "original_file_path": "models/staging/stripe/_stripe__sources.yml",
            "description": "Raw payment events",
            "freshness": {
                "warn_after": {"count": 24, "period": "hour"},
                "error_after": None,
            },
            "meta": {},
            "database": "raw",
            "schema": "stripe",
            "identifier": "payments",
        }

    def test_basic_fields(self, source_node):
        resource = _source_to_resource(source_node)
        assert resource.resource_id == "source.pkg.stripe.payments"
        assert resource.resource_name == "payments"
        assert resource.resource_type == "source"
        assert resource.file_path == "models/staging/stripe/_stripe__sources.yml"

    def test_source_described(self, source_node):
        resource = _source_to_resource(source_node)
        assert resource.is_described is True

    def test_source_undescribed(self, source_node):
        source_node["description"] = ""
        resource = _source_to_resource(source_node)
        assert resource.is_described is False

    def test_freshness_enabled(self, source_node):
        resource = _source_to_resource(source_node)
        assert resource.is_freshness_enabled is True

    def test_freshness_disabled(self, source_node):
        source_node["freshness"] = {"warn_after": None, "error_after": None}
        resource = _source_to_resource(source_node)
        assert resource.is_freshness_enabled is False

    def test_freshness_null(self, source_node):
        source_node["freshness"] = None
        resource = _source_to_resource(source_node)
        assert resource.is_freshness_enabled is False

    def test_model_type_empty_for_sources(self, source_node):
        resource = _source_to_resource(source_node)
        assert resource.model_type == ""

    def test_source_meta_contains_source_description(self, source_node):
        """source_description_populated tracks source-level description."""
        resource = _source_to_resource(source_node)
        assert resource.meta["source_description_populated"] is True

    def test_source_meta_no_description(self, source_node):
        source_node["source_description"] = ""
        resource = _source_to_resource(source_node)
        assert resource.meta["source_description_populated"] is False

    def test_source_empty_raw_code(self, source_node):
        resource = _source_to_resource(source_node)
        assert resource.raw_code == ""

    def test_source_empty_config(self, source_node):
        resource = _source_to_resource(source_node)
        assert not resource.config

    def test_source_columns(self, source_node):
        source_node["columns"] = {
            "id": {"name": "id", "description": "PK", "data_type": "integer"}
        }
        resource = _source_to_resource(source_node)
        assert len(resource.columns) == 1
        assert resource.columns[0].name == "id"

    def test_source_no_columns(self, source_node):
        resource = _source_to_resource(source_node)
        assert not resource.columns


class TestExposureToResource:
    @pytest.fixture
    def exposure_node(self):
        return {
            "unique_id": "exposure.pkg.weekly_report",
            "name": "weekly_report",
            "original_file_path": "models/exposures/weekly_report.yml",
            "depends_on": {
                "nodes": ["model.pkg.fct_orders", "model.pkg.dim_customers"]
            },
        }

    def test_basic_fields(self, exposure_node):
        resource = _exposure_to_resource(exposure_node)
        assert resource.resource_id == "exposure.pkg.weekly_report"
        assert resource.resource_name == "weekly_report"
        assert resource.resource_type == "exposure"
        assert resource.file_path == "models/exposures/weekly_report.yml"

    def test_exposure_defaults(self, exposure_node):
        resource = _exposure_to_resource(exposure_node)
        assert resource.model_type == ""
        assert resource.materialization == ""
        assert resource.is_described is False
        assert resource.is_public is False
        assert resource.number_of_columns == 0
        assert resource.raw_code == ""
        assert not resource.config
        assert not resource.columns


class TestExtractEdges:
    def test_empty_parent_map(self):
        assert not _extract_edges({})

    def test_single_edge(self):
        parent_map = {"model.pkg.stg_orders": ["source.pkg.stripe.orders"]}
        edges = _extract_edges(parent_map)
        assert len(edges) == 1
        assert edges[0] == DirectEdge(
            parent="source.pkg.stripe.orders", child="model.pkg.stg_orders"
        )

    def test_multiple_parents(self):
        parent_map = {
            "model.pkg.fct_orders": [
                "model.pkg.stg_orders",
                "model.pkg.stg_customers",
            ]
        }
        edges = _extract_edges(parent_map)
        assert len(edges) == 2
        parents = {e.parent for e in edges}
        assert parents == {"model.pkg.stg_orders", "model.pkg.stg_customers"}

    def test_multiple_children(self):
        parent_map = {
            "model.pkg.stg_orders": ["source.pkg.stripe.orders"],
            "model.pkg.stg_payments": ["source.pkg.stripe.payments"],
        }
        edges = _extract_edges(parent_map)
        assert len(edges) == 2


class TestParseManifest:
    """Integration test: full parse_manifest with minimal fixture."""

    @pytest.fixture
    def manifest_dict(self):
        return {
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"
            },
            "nodes": {
                "model.pkg.stg_orders": {
                    "unique_id": "model.pkg.stg_orders",
                    "name": "stg_orders",
                    "resource_type": "model",
                    "original_file_path": "models/staging/stripe/stg_orders.sql",
                    "fqn": ["pkg", "staging", "stripe", "stg_orders"],
                    "config": {"materialized": "view", "meta": {}, "tags": []},
                    "description": "Staged orders",
                    "columns": {"id": {"name": "id", "description": "PK"}},
                    "raw_code": "SELECT * FROM {{ source('stripe', 'orders') }}",
                    "access": "protected",
                    "contract": {"enforced": False},
                    "schema": "staging",
                    "database": "analytics",
                },
                "test.pkg.unique_stg_orders_id": {
                    "resource_type": "test",
                    "test_metadata": {
                        "name": "unique",
                        "namespace": "dbt",
                        "kwargs": {},
                    },
                    "attached_node": "model.pkg.stg_orders",
                },
                "test.pkg.not_null_stg_orders_id": {
                    "resource_type": "test",
                    "test_metadata": {
                        "name": "not_null",
                        "namespace": "dbt",
                        "kwargs": {},
                    },
                    "attached_node": "model.pkg.stg_orders",
                },
            },
            "sources": {
                "source.pkg.stripe.orders": {
                    "unique_id": "source.pkg.stripe.orders",
                    "name": "orders",
                    "source_name": "stripe",
                    "source_description": "Stripe data",
                    "original_file_path": "models/staging/stripe/_stripe__sources.yml",
                    "description": "Raw orders",
                    "freshness": {
                        "warn_after": {"count": 24, "period": "hour"},
                        "error_after": None,
                    },
                    "meta": {},
                    "database": "raw",
                    "schema": "stripe",
                    "identifier": "orders",
                },
            },
            "exposures": {
                "exposure.pkg.dashboard": {
                    "unique_id": "exposure.pkg.dashboard",
                    "name": "dashboard",
                    "original_file_path": "models/exposures/dashboard.yml",
                    "depends_on": {"nodes": ["model.pkg.stg_orders"]},
                },
            },
            "parent_map": {
                "model.pkg.stg_orders": ["source.pkg.stripe.orders"],
                "exposure.pkg.dashboard": ["model.pkg.stg_orders"],
            },
        }

    def test_parse_returns_resources_and_edges(self, manifest_dict, tmp_path):
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_dict))
        config = load_config(None)
        resources, edges = parse_manifest(manifest_path, config)

        assert len(resources) == 3  # 1 model + 1 source + 1 exposure
        assert len(edges) == 2

        resource_types = {resource.resource_type for resource in resources}
        assert resource_types == {"model", "source", "exposure"}

    def test_model_fields_correct(self, manifest_dict, tmp_path):
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_dict))
        config = load_config(None)
        resources, _ = parse_manifest(manifest_path, config)

        model = next(
            resource for resource in resources if resource.resource_type == "model"
        )
        assert model.resource_name == "stg_orders"
        assert model.model_type == "staging"
        assert model.is_primary_key_tested is True
        assert model.hard_coded_references is False

    def test_version_guard_rejects_old_manifest(self, manifest_dict, tmp_path):
        manifest_dict["metadata"]["dbt_schema_version"] = (
            "https://schemas.getdbt.com/dbt/manifest/v9.json"
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_dict))
        config = load_config(None)
        with pytest.raises(SystemExit, match="v11"):
            parse_manifest(manifest_path, config)
