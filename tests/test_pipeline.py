"""End-to-end pipeline test: parse_manifest -> build_relationships -> evaluate.

Exercises the full pipeline with a minimal fixture manifest that triggers
known rule violations, verifying that all stages integrate correctly.
"""

from __future__ import annotations

import json

import pytest

from dbt_lint.config import load_config
from dbt_lint.engine import evaluate
from dbt_lint.graph import build_relationships
from dbt_lint.manifest import parse_manifest
from helpers import fixture_manifest_dict


class TestEndToEndPipeline:
    """Full pipeline: manifest JSON -> parse -> graph -> evaluate."""

    @pytest.fixture
    def pipeline(self, tmp_path):
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(fixture_manifest_dict()))

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
