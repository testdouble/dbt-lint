"""Unit tests for filter operations: rule-ID, resource-eligibility, violation-exclusion."""

from dbt_lint.config import Config, RuleConfig
from dbt_lint.filters import (
    filter_rules_by_id,
    filter_violations_by_resource_exclusions,
    is_resource_excluded_from_rule,
)


def _default_config(include: str | None = None, exclude: str | None = None) -> Config:
    return Config(
        params={},
        include=include,
        exclude=exclude,
        config_dir=None,
        _rule_overrides={},
        _custom_rule_entries=[],
    )


def _rule_config(exclude_resources: list[str] | None = None) -> RuleConfig:
    return RuleConfig(exclude_resources=exclude_resources or [])


class TestFilterRulesById:
    def test_empty_select_and_exclude_returns_input_unchanged(self, make_rule):
        rules = [make_rule("a/one"), make_rule("b/two")]

        result = filter_rules_by_id(rules, select=(), exclude=())

        assert result == rules

    def test_select_keeps_only_matching_rule_ids(self, make_rule):
        rules = [make_rule("a/one"), make_rule("b/two"), make_rule("a/three")]

        result = filter_rules_by_id(rules, select=("a/one", "a/three"), exclude=())

        assert [rule.id for rule in result] == ["a/one", "a/three"]

    def test_exclude_drops_matching_rule_ids(self, make_rule):
        rules = [make_rule("a/one"), make_rule("b/two"), make_rule("a/three")]

        result = filter_rules_by_id(rules, select=(), exclude=("b/two",))

        assert [rule.id for rule in result] == ["a/one", "a/three"]

    def test_select_and_exclude_combine(self, make_rule):
        rules = [make_rule("a/one"), make_rule("b/two"), make_rule("a/three")]

        result = filter_rules_by_id(
            rules, select=("a/one", "a/three"), exclude=("a/three",)
        )

        assert [rule.id for rule in result] == ["a/one"]


class TestIsResourceExcludedFromRule:
    def test_meta_skip_excludes_resource(self, make_resource):
        resource = make_resource(skip_rules=frozenset(["a/rule"]))

        result = is_resource_excluded_from_rule(
            resource, "a/rule", _rule_config(), _default_config()
        )

        assert result

    def test_exclude_resources_glob_match_excludes(self, make_resource):
        resource = make_resource(resource_id="model.pkg.legacy_orders")
        rule_config = _rule_config(exclude_resources=["model.pkg.legacy_*"])

        result = is_resource_excluded_from_rule(
            resource, "a/rule", rule_config, _default_config()
        )

        assert result

    def test_exclude_resources_glob_no_match_does_not_exclude(self, make_resource):
        resource = make_resource(resource_id="model.pkg.stg_orders")
        rule_config = _rule_config(exclude_resources=["model.pkg.legacy_*"])

        result = is_resource_excluded_from_rule(
            resource, "a/rule", rule_config, _default_config()
        )

        assert not result

    def test_include_path_filter_excludes_when_path_outside_include(
        self, make_resource
    ):
        outside = make_resource(file_path="models/marts/orders.sql")
        config = _default_config(include="models/staging/.*")

        result = is_resource_excluded_from_rule(
            outside, "a/rule", _rule_config(), config
        )

        assert result

    def test_include_path_filter_admits_matching_path(self, make_resource):
        inside = make_resource(file_path="models/staging/orders.sql")
        config = _default_config(include="models/staging/.*")

        result = is_resource_excluded_from_rule(
            inside, "a/rule", _rule_config(), config
        )

        assert not result

    def test_exclude_path_filter_excludes_matching_path(self, make_resource):
        resource = make_resource(file_path="models/legacy/orders.sql")
        config = _default_config(exclude="models/legacy/.*")

        result = is_resource_excluded_from_rule(
            resource, "a/rule", _rule_config(), config
        )

        assert result


class TestFilterViolationsByResourceExclusions:
    def test_empty_exclude_resources_returns_input_unchanged(self, make_violation):
        violations = [make_violation(), make_violation()]

        result = filter_violations_by_resource_exclusions(violations, _rule_config())

        assert result == violations

    def test_drops_violations_for_matching_resource_glob(self, make_violation):
        legacy = make_violation(resource_id="model.pkg.legacy_a")
        current = make_violation(resource_id="model.pkg.stg_a")
        rule_config = _rule_config(exclude_resources=["model.pkg.legacy_*"])

        result = filter_violations_by_resource_exclusions(
            [legacy, current], rule_config
        )

        assert result == [current]
