"""Tests for @rule decorator, signature detection, registry, and helpers."""

from dbt_linter.config import RuleConfig
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import (
    RuleDef,
    RuleInfo,
    direct_edges,
    filter_by_model_type,
    generate_rules_index,
    get_all_rules,
    group_by,
    rule,
)


class TestRuleDecorator:
    def test_stores_metadata(self):
        @rule(
            id="testing/example-rule",
            description="An example rule.",
        )
        def example_rule(resource: Resource, config: RuleConfig) -> Violation | None:
            return None

        assert hasattr(example_rule, "_rule_meta")
        assert example_rule._rule_meta.id == "testing/example-rule"
        assert example_rule._rule_meta.description == "An example rule."

    def test_returns_function_unchanged(self):
        @rule(id="testing/noop", description="Noop.")
        def noop(resource: Resource, config: RuleConfig) -> Violation | None:
            return None

        # Function is still callable
        assert callable(noop)

    def test_category_derived_from_id(self):
        @rule(id="modeling/some-rule", description="Test.")
        def some_rule(resource: Resource, config: RuleConfig) -> Violation | None:
            return None

        assert some_rule._rule_meta.category == "modeling"


class TestSignatureDetection:
    def test_per_resource_rule(self):
        @rule(id="test/per-resource", description="Test.")
        def per_resource(resource: Resource, config: RuleConfig) -> Violation | None:
            return None

        rd = RuleDef.from_function(per_resource)
        assert rd.is_per_resource is True

    def test_aggregate_rule(self):
        @rule(id="test/aggregate", description="Test.")
        def aggregate(
            resources: list[Resource],
            relationships: list[Relationship],
            config: RuleConfig,
        ) -> list[Violation]:
            return []

        rd = RuleDef.from_function(aggregate)
        assert rd.is_per_resource is False


class TestGroupBy:
    def test_groups_items(self):
        items = [("a", 1), ("b", 2), ("a", 3)]
        result = group_by(items, key=lambda x: x[0])
        assert result == {"a": [("a", 1), ("a", 3)], "b": [("b", 2)]}

    def test_empty_input(self):
        assert group_by([], key=lambda x: x) == {}


class TestFilterByModelType:
    def test_filters(self, make_resource):
        resources = [
            make_resource(model_type="staging"),
            make_resource(model_type="marts"),
            make_resource(model_type="staging"),
        ]
        result = filter_by_model_type(resources, "staging")
        assert len(result) == 2
        assert all(r.model_type == "staging" for r in result)

    def test_no_matches(self, make_resource):
        resources = [make_resource(model_type="marts")]
        assert filter_by_model_type(resources, "staging") == []


class TestDirectEdges:
    def test_filters_distance_one(self, make_relationship):
        rels = [
            make_relationship(distance=1),
            make_relationship(distance=2),
            make_relationship(distance=1),
        ]
        result = direct_edges(rels)
        assert len(result) == 2
        assert all(r.distance == 1 for r in result)


class TestGetAllRules:
    def test_discovers_all_41_rules(self):
        rules = get_all_rules()
        assert len(rules) == 41

    def test_all_rules_have_unique_ids(self):
        rules = get_all_rules()
        ids = [r.id for r in rules]
        assert len(ids) == len(set(ids))

    def test_all_categories_present(self):
        rules = get_all_rules()
        categories = {r.category for r in rules}
        assert categories == {
            "modeling",
            "testing",
            "documentation",
            "structure",
            "performance",
            "governance",
        }


class TestAllRulesHaveDocstrings:
    def test_all_rules_have_docstrings(self):
        rules = get_all_rules()
        for r in rules:
            assert r.fn.__doc__, f"{r.id} is missing a docstring"


class TestGenerateRulesIndex:
    def test_returns_41_entries(self):
        index = generate_rules_index()
        assert len(index) == 41

    def test_sorted_by_id(self):
        index = generate_rules_index()
        ids = [r.id for r in index]
        assert ids == sorted(ids)

    def test_all_have_description_and_category(self):
        index = generate_rules_index()
        for info in index:
            assert info.description, f"{info.id} missing description"
            assert info.category, f"{info.id} missing category"

    def test_categories_match_id_prefix(self):
        index = generate_rules_index()
        for info in index:
            assert info.id.startswith(info.category + "/")

    def test_returns_rule_info_instances(self):
        index = generate_rules_index()
        for info in index:
            assert isinstance(info, RuleInfo)

    def test_is_per_resource_is_bool(self):
        index = generate_rules_index()
        for info in index:
            assert isinstance(info.is_per_resource, bool)
