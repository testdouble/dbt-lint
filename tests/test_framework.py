"""Unit tests for @rule decorator, signature detection, and helpers."""

from dbt_lint.models import Relationship, Resource, Violation
from dbt_lint.rules import (
    RuleContext,
    RuleDef,
    RuleMeta,
    direct_edges,
    filter_by_model_type,
    group_by,
    resolve_name,
    resources_by_id,
    rule,
)


class TestRuleDecorator:
    def test_stores_metadata(self):
        @rule(
            id="testing/example-rule",
            description="An example rule.",
        )
        def example_rule(resource: Resource, context: RuleContext) -> Violation | None:
            return None

        assert hasattr(example_rule, "_rule_meta")
        assert example_rule._rule_meta.id == "testing/example-rule"
        assert example_rule._rule_meta.description == "An example rule."

    def test_returns_function_unchanged(self):
        @rule(id="testing/noop", description="Noop.")
        def noop(resource: Resource, context: RuleContext) -> Violation | None:
            return None

        # Function is still callable
        assert callable(noop)

    def test_category_derived_from_id(self):
        @rule(id="modeling/some-rule", description="Test.")
        def some_rule(resource: Resource, context: RuleContext) -> Violation | None:
            return None

        assert some_rule._rule_meta.category == "modeling"

    def test_structured_metadata_kwargs(self):
        @rule(
            id="testing/structured",
            description="Structured.",
            rationale="Because reasons.",
            remediation="Fix it.",
            exceptions="When X.",
            examples=["Violation: bad", "Pass: good"],
        )
        def structured(resource: Resource, context: RuleContext) -> Violation | None:
            return None

        meta: RuleMeta = structured._rule_meta
        assert meta.rationale == "Because reasons."
        assert meta.remediation == "Fix it."
        assert meta.exceptions == "When X."
        assert meta.examples == ("Violation: bad", "Pass: good")

    def test_structured_metadata_defaults(self):
        @rule(id="testing/defaults", description="Defaults.")
        def defaults(resource: Resource, context: RuleContext) -> Violation | None:
            return None

        meta: RuleMeta = defaults._rule_meta
        assert meta.rationale == ""
        assert meta.remediation == ""
        assert meta.exceptions == ""
        assert not meta.examples


class TestSignatureDetection:
    def test_per_resource_rule(self):
        @rule(id="test/per-resource", description="Test.")
        def per_resource(resource: Resource, context: RuleContext) -> Violation | None:
            return None

        rd = RuleDef.from_function(per_resource)
        assert rd.is_per_resource is True

    def test_aggregate_rule(self):
        @rule(id="test/aggregate", description="Test.")
        def aggregate(
            resources: list[Resource],
            relationships: list[Relationship],
            context: RuleContext,
        ) -> list[Violation]:
            return []

        rd = RuleDef.from_function(aggregate)
        assert rd.is_per_resource is False


class TestGroupBy:
    def test_groups_items(self):
        items = [("a", 1), ("b", 2), ("a", 3)]
        grouped = group_by(items, key=lambda x: x[0])
        assert grouped == {"a": [("a", 1), ("a", 3)], "b": [("b", 2)]}

    def test_empty_input(self):
        assert not group_by([], key=lambda x: x)


class TestFilterByModelType:
    def test_filters(self, make_resource):
        resources = [
            make_resource(model_type="staging"),
            make_resource(model_type="marts"),
            make_resource(model_type="staging"),
        ]
        filtered = filter_by_model_type(resources, "staging")
        assert len(filtered) == 2
        assert all(r.model_type == "staging" for r in filtered)

    def test_no_matches(self, make_resource):
        resources = [make_resource(model_type="marts")]
        assert not filter_by_model_type(resources, "staging")


class TestResourcesById:
    def test_indexes_by_id(self, make_resource):
        orders = make_resource(resource_id="model.pkg.orders")
        customers = make_resource(resource_id="model.pkg.customers")
        by_id = resources_by_id([orders, customers])
        assert by_id["model.pkg.orders"] is orders
        assert by_id["model.pkg.customers"] is customers

    def test_empty_input(self):
        assert resources_by_id([]) == {}

    def test_duplicate_ids_last_wins(self, make_resource):
        original = make_resource(resource_id="model.pkg.orders", resource_name="first")
        replacement = make_resource(
            resource_id="model.pkg.orders", resource_name="second"
        )
        by_id = resources_by_id([original, replacement])
        assert by_id["model.pkg.orders"].resource_name == "second"


class TestResolveName:
    def test_found_returns_name(self, make_resource):
        orders = make_resource(resource_id="model.pkg.orders", resource_name="orders")
        by_id = resources_by_id([orders])
        assert resolve_name(by_id, "model.pkg.orders") == "orders"

    def test_missing_returns_raw_id(self):
        assert resolve_name({}, "model.pkg.unknown") == "model.pkg.unknown"

    def test_empty_dict(self):
        assert resolve_name({}, "anything") == "anything"


class TestDirectEdges:
    def test_filters_distance_one(self, make_relationship):
        rels = [
            make_relationship(distance=1),
            make_relationship(distance=2),
            make_relationship(distance=1),
        ]
        filtered = direct_edges(rels)
        assert len(filtered) == 2
        assert all(r.distance == 1 for r in filtered)
