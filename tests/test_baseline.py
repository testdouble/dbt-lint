"""Tests for baseline generation: violations -> YAML config."""

from __future__ import annotations

import yaml

from dbt_linter.baseline import generate_baseline


class TestGenerateBaseline:
    """Unit tests for generate_baseline()."""

    def test_empty_violations_returns_empty_rules(self):
        result = generate_baseline([])
        parsed = yaml.safe_load(result)
        assert parsed == {"rules": {}}

    def test_single_rule_single_resource(self, make_violation):
        violations = [
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.stg_users",
            ),
        ]
        result = generate_baseline(violations)
        parsed = yaml.safe_load(result)
        assert parsed == {
            "rules": {
                "documentation/undocumented-models": {
                    "exclude_resources": ["model.pkg.stg_users"],
                },
            },
        }

    def test_single_rule_multiple_resources(self, make_violation):
        violations = [
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.stg_users",
            ),
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.fct_orders",
            ),
        ]
        result = generate_baseline(violations)
        parsed = yaml.safe_load(result)
        rule = parsed["rules"]["documentation/undocumented-models"]
        resources = rule["exclude_resources"]
        assert sorted(resources) == ["model.pkg.fct_orders", "model.pkg.stg_users"]

    def test_multiple_rules(self, make_violation):
        violations = [
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.stg_users",
            ),
            make_violation(
                rule_id="testing/missing-primary-key-tests",
                resource_id="model.pkg.fct_orders",
            ),
        ]
        result = generate_baseline(violations)
        parsed = yaml.safe_load(result)
        assert "documentation/undocumented-models" in parsed["rules"]
        assert "testing/missing-primary-key-tests" in parsed["rules"]

    def test_deduplicates_resources(self, make_violation):
        """Same resource violating the same rule twice should appear once."""
        violations = [
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.stg_users",
            ),
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.stg_users",
            ),
        ]
        result = generate_baseline(violations)
        parsed = yaml.safe_load(result)
        rule = parsed["rules"]["documentation/undocumented-models"]
        resources = rule["exclude_resources"]
        assert resources == ["model.pkg.stg_users"]

    def test_rules_sorted_alphabetically(self, make_violation):
        violations = [
            make_violation(
                rule_id="testing/missing-primary-key-tests",
                resource_id="model.pkg.a",
            ),
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.b",
            ),
            make_violation(
                rule_id="modeling/source-fanout",
                resource_id="model.pkg.c",
            ),
        ]
        result = generate_baseline(violations)
        parsed = yaml.safe_load(result)
        rule_ids = list(parsed["rules"].keys())
        assert rule_ids == sorted(rule_ids)

    def test_resources_sorted_within_rule(self, make_violation):
        violations = [
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.z_model",
            ),
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.a_model",
            ),
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.m_model",
            ),
        ]
        result = generate_baseline(violations)
        parsed = yaml.safe_load(result)
        rule = parsed["rules"]["documentation/undocumented-models"]
        resources = rule["exclude_resources"]
        assert resources == [
            "model.pkg.a_model",
            "model.pkg.m_model",
            "model.pkg.z_model",
        ]

    def test_output_is_valid_yaml(self, make_violation):
        violations = [
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.stg_users",
            ),
            make_violation(
                rule_id="testing/missing-primary-key-tests",
                resource_id="model.pkg.fct_orders",
            ),
        ]
        result = generate_baseline(violations)
        parsed = yaml.safe_load(result)
        assert isinstance(parsed, dict)
        assert "rules" in parsed

    def test_output_contains_header_comment(self):
        result = generate_baseline([])
        assert "Generated by dbt-lint" in result

    def test_synthetic_ids_emit_enabled_false(self, make_violation):
        """Rules with only synthetic resource_ids get disabled."""
        violations = [
            make_violation(
                rule_id="documentation/documentation-coverage",
                resource_id="model_type:staging",
            ),
            make_violation(
                rule_id="documentation/documentation-coverage",
                resource_id="model_type:marts",
            ),
        ]
        result = generate_baseline(violations)
        parsed = yaml.safe_load(result)
        rule = parsed["rules"]["documentation/documentation-coverage"]
        assert rule == {"enabled": False}

    def test_mixed_real_and_synthetic_ids(self, make_violation):
        """Rules with both real and synthetic IDs only exclude real ones."""
        violations = [
            make_violation(
                rule_id="testing/test-coverage",
                resource_id="model_type:staging",
            ),
            make_violation(
                rule_id="testing/missing-primary-key-tests",
                resource_id="model.pkg.a",
            ),
        ]
        result = generate_baseline(violations)
        parsed = yaml.safe_load(result)
        assert parsed["rules"]["testing/test-coverage"] == {
            "enabled": False,
        }
        assert parsed["rules"]["testing/missing-primary-key-tests"] == {
            "exclude_resources": ["model.pkg.a"],
        }
