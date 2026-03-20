"""End-to-end pipeline test: parse_manifest -> build_relationships -> evaluate.

Exercises the full pipeline with a minimal fixture manifest that triggers
known rule violations, verifying that all stages integrate correctly.
"""

from __future__ import annotations

import json

import pytest

from dbt_linter.config import load_config
from dbt_linter.engine import evaluate
from dbt_linter.graph import build_relationships
from dbt_linter.manifest import parse_manifest


def _fixture_manifest() -> dict:
    """Build a minimal valid manifest.json dict.

    Graph shape:
        source.pkg.raw.users
            -> model.pkg.stg_users (view, described, PK tested)
                -> model.pkg.fct_orders (table, NOT described, public, no contract)
                    -> exposure.pkg.dashboard

    Expected violations from default config:
    - documentation/undocumented-models: fct_orders has no description
    - governance/public-models-without-contract: fct_orders is public w/o contract
    - governance/undocumented-public-models: fct_orders is public but undocumented
    - governance/exposures-depend-on-private-models: should NOT fire
      (fct_orders is public)
    """
    return {
        "metadata": {
            "dbt_schema_version": ("https://schemas.getdbt.com/dbt/manifest/v11.json"),
        },
        "nodes": {
            "model.pkg.stg_users": {
                "unique_id": "model.pkg.stg_users",
                "name": "stg_users",
                "resource_type": "model",
                "original_file_path": "models/staging/stg_users.sql",
                "raw_code": "select * from {{ ref('raw_users') }}",
                "description": "Staged users from raw source.",
                "schema": "public",
                "database": "analytics",
                "access": "protected",
                "contract": {"enforced": False},
                "config": {
                    "materialized": "view",
                    "tags": [],
                    "meta": {},
                },
                "columns": {
                    "user_id": {
                        "name": "user_id",
                        "description": "Primary key",
                    },
                },
            },
            "model.pkg.fct_orders": {
                "unique_id": "model.pkg.fct_orders",
                "name": "fct_orders",
                "resource_type": "model",
                "original_file_path": "models/marts/fct_orders.sql",
                "raw_code": "select * from {{ ref('stg_users') }}",
                "description": "",
                "schema": "public",
                "database": "analytics",
                "access": "public",
                "contract": {"enforced": False},
                "config": {
                    "materialized": "table",
                    "tags": [],
                    "meta": {},
                },
                "columns": {
                    "order_id": {
                        "name": "order_id",
                        "description": "",
                    },
                },
            },
            # Test nodes for PK test derivation
            "test.pkg.unique_stg_users_user_id": {
                "unique_id": "test.pkg.unique_stg_users_user_id",
                "name": "unique_stg_users_user_id",
                "resource_type": "test",
                "test_metadata": {
                    "name": "unique",
                    "namespace": "dbt",
                },
                "attached_node": "model.pkg.stg_users",
            },
            "test.pkg.not_null_stg_users_user_id": {
                "unique_id": "test.pkg.not_null_stg_users_user_id",
                "name": "not_null_stg_users_user_id",
                "resource_type": "test",
                "test_metadata": {
                    "name": "not_null",
                    "namespace": "dbt",
                },
                "attached_node": "model.pkg.stg_users",
            },
        },
        "sources": {
            "source.pkg.raw.users": {
                "unique_id": "source.pkg.raw.users",
                "name": "users",
                "resource_type": "source",
                "original_file_path": "models/staging/_sources.yml",
                "source_description": "Raw user data from app DB.",
                "description": "Users table in raw schema.",
                "schema": "raw",
                "database": "analytics",
                "meta": {},
                "freshness": {
                    "warn_after": {"count": 24, "period": "hour"},
                    "error_after": None,
                },
            },
        },
        "exposures": {
            "exposure.pkg.dashboard": {
                "unique_id": "exposure.pkg.dashboard",
                "name": "dashboard",
                "resource_type": "exposure",
                "original_file_path": "models/exposures/dashboard.yml",
            },
        },
        "parent_map": {
            "model.pkg.stg_users": ["source.pkg.raw.users"],
            "model.pkg.fct_orders": ["model.pkg.stg_users"],
            "exposure.pkg.dashboard": ["model.pkg.fct_orders"],
        },
    }


class TestEndToEndPipeline:
    """Full pipeline: manifest JSON -> parse -> graph -> evaluate."""

    @pytest.fixture
    def pipeline(self, tmp_path):
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(_fixture_manifest()))

        config = load_config(None)
        resources, edges = parse_manifest(manifest_path, config)
        relationships = build_relationships(resources, edges)
        result = evaluate(resources, relationships, config)
        return resources, edges, relationships, result.violations

    def test_parse_extracts_all_resources(self, pipeline):
        resources, _, _, _ = pipeline
        ids = {r.resource_id for r in resources}
        assert "source.pkg.raw.users" in ids
        assert "model.pkg.stg_users" in ids
        assert "model.pkg.fct_orders" in ids
        assert "exposure.pkg.dashboard" in ids
        assert len(resources) == 4

    def test_parse_extracts_edges(self, pipeline):
        _, edges, _, _ = pipeline
        edge_pairs = {(e.parent, e.child) for e in edges}
        assert ("source.pkg.raw.users", "model.pkg.stg_users") in edge_pairs
        assert ("model.pkg.stg_users", "model.pkg.fct_orders") in edge_pairs
        assert ("model.pkg.fct_orders", "exposure.pkg.dashboard") in edge_pairs

    def test_graph_builds_transitive_relationships(self, pipeline):
        _, _, relationships, _ = pipeline
        pairs = {(r.parent, r.child): r for r in relationships}

        # Direct edges
        assert pairs[("source.pkg.raw.users", "model.pkg.stg_users")].distance == 1
        assert pairs[("model.pkg.stg_users", "model.pkg.fct_orders")].distance == 1
        assert pairs[("model.pkg.fct_orders", "exposure.pkg.dashboard")].distance == 1

        # Transitive edges
        assert pairs[("source.pkg.raw.users", "model.pkg.fct_orders")].distance == 2
        assert pairs[("source.pkg.raw.users", "exposure.pkg.dashboard")].distance == 3
        assert pairs[("model.pkg.stg_users", "exposure.pkg.dashboard")].distance == 2

        assert len(relationships) == 6

    def test_model_type_classification(self, pipeline):
        resources, _, _, _ = pipeline
        by_id = {r.resource_id: r for r in resources}
        assert by_id["model.pkg.stg_users"].model_type == "staging"
        assert by_id["model.pkg.fct_orders"].model_type == "marts"

    def test_pk_test_derivation(self, pipeline):
        resources, _, _, _ = pipeline
        by_id = {r.resource_id: r for r in resources}
        assert by_id["model.pkg.stg_users"].is_primary_key_tested is True
        assert by_id["model.pkg.fct_orders"].is_primary_key_tested is False

    def test_source_freshness_flag(self, pipeline):
        resources, _, _, _ = pipeline
        by_id = {r.resource_id: r for r in resources}
        assert by_id["source.pkg.raw.users"].is_freshness_enabled is True

    def test_undocumented_model_violation(self, pipeline):
        _, _, _, violations = pipeline
        undoc = [
            violation
            for violation in violations
            if violation.rule_id == "documentation/undocumented-models"
        ]
        assert len(undoc) == 1
        assert undoc[0].resource_id == "model.pkg.fct_orders"

    def test_public_model_without_contract_violation(self, pipeline):
        _, _, _, violations = pipeline
        no_contract = [
            violation
            for violation in violations
            if violation.rule_id == "governance/public-models-without-contract"
        ]
        assert len(no_contract) == 1
        assert no_contract[0].resource_id == "model.pkg.fct_orders"

    def test_undocumented_public_model_violation(self, pipeline):
        _, _, _, violations = pipeline
        undoc_pub = [
            violation
            for violation in violations
            if violation.rule_id == "governance/undocumented-public-models"
        ]
        assert len(undoc_pub) == 1
        assert "fct_orders" in undoc_pub[0].message

    def test_exposure_depends_on_public_model_no_violation(self, pipeline):
        """fct_orders is public, so this rule should NOT fire."""
        _, _, _, violations = pipeline
        priv_exp = [
            violation
            for violation in violations
            if violation.rule_id == "governance/exposures-depend-on-private-models"
        ]
        assert len(priv_exp) == 0

    def test_chain_of_views_in_relationships(self, pipeline):
        _, _, relationships, _ = pipeline
        pairs = {(r.parent, r.child): r for r in relationships}

        # source -> stg_users(view) -> fct_orders: intermediate is view
        rel = pairs[("source.pkg.raw.users", "model.pkg.fct_orders")]
        assert rel.is_dependent_on_chain_of_views is True

    def test_all_violations_have_rule_id_and_severity(self, pipeline):
        _, _, _, violations = pipeline
        for violation in violations:
            assert violation.rule_id, f"Missing rule_id on violation: {violation}"
            assert violation.severity, f"Missing severity on violation: {violation}"
