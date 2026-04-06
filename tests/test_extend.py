"""Tests for the public API facade: dbt_lint.extend."""

from dbt_lint import extend


class TestAllExports:
    def test_all_exports_are_importable(self):
        for name in extend.__all__:
            assert hasattr(extend, name), f"{name} not found in extend"

    def test_expected_exports(self):
        expected = {
            "ColumnInfo",
            "Relationship",
            "Resource",
            "RuleConfig",
            "Violation",
            "direct_edges",
            "filter_by_model_type",
            "group_by",
            "rule",
        }
        assert set(extend.__all__) == expected

    def test_rule_is_callable(self):
        assert callable(extend.rule)

    def test_violation_from_resource_available(self):
        assert hasattr(extend.Violation, "from_resource")
