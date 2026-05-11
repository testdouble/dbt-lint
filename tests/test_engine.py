"""Unit tests for rule engine: dispatch, gating, context plumbing, fail-fast."""

from dbt_lint.config import DEFAULTS, Config
from dbt_lint.engine import evaluate
from dbt_lint.models import Violation

STUB_PER_RESOURCE_ID = "test/stub-per-resource"
STUB_AGGREGATE_ID = "test/stub-aggregate"


def _stub_per_resource(resource, context):
    """Per-resource stub: always returns a violation."""
    return context.violation(resource, "stub violation")


def _stub_per_resource_clean(resource, context):
    """Per-resource stub: never returns a violation."""
    return None


def _stub_aggregate(resources, relationships, context):
    """Aggregate stub: returns one violation per resource."""
    return [context.violation(r, "agg violation") for r in resources]


def _default_config(**rule_overrides) -> Config:
    """Build a Config directly, without touching YAML or DEFAULTS."""

    return Config(
        params={**DEFAULTS},
        include=None,
        exclude=None,
        config_dir=None,
        _rule_overrides=rule_overrides,
        _custom_rule_entries=[],
    )


class TestEngineDispatch:
    """Engine dispatch and gating with injected stub rules."""

    def test_per_resource_dispatches_to_each_resource(self, make_resource, make_rule):
        resources = [make_resource(), make_resource(), make_resource()]
        rule = make_rule(STUB_PER_RESOURCE_ID, fn=_stub_per_resource)

        result = evaluate(resources, [], _default_config(), rules=[rule])

        assert len(result.violations) == 3

    def test_aggregate_dispatches_with_resources_and_relationships(
        self, make_resource, make_relationship, make_rule
    ):
        resources = [make_resource(), make_resource()]
        rels = [make_relationship()]
        captured = {}

        def spy_aggregate(res, relationships, context):
            captured["resources"] = res
            captured["relationships"] = relationships
            return [context.violation(res[0], "agg")]

        rule = make_rule(STUB_AGGREGATE_ID, fn=spy_aggregate, is_per_resource=False)

        result = evaluate(resources, rels, _default_config(), rules=[rule])

        assert captured["resources"] == resources
        assert captured["relationships"] == rels
        assert len(result.violations) == 1

    def test_disabled_rule_produces_no_violations(self, make_resource, make_rule):
        resources = [make_resource()]
        config = _default_config(**{STUB_PER_RESOURCE_ID: {"enabled": False}})
        rule = make_rule(STUB_PER_RESOURCE_ID, fn=_stub_per_resource)

        result = evaluate(resources, [], config, rules=[rule])

        assert not result.violations


class TestEngineContextPlumbing:
    """Engine constructs the RuleContext that rules use to build violations."""

    def test_context_carries_rule_id_and_default_severity(
        self, make_resource, make_rule
    ):
        resources = [make_resource()]
        rule = make_rule(STUB_PER_RESOURCE_ID, fn=_stub_per_resource)

        result = evaluate(resources, [], _default_config(), rules=[rule])

        violation = result.violations[0]
        assert violation.rule_id == STUB_PER_RESOURCE_ID
        assert violation.severity == "warn"

    def test_severity_override_from_config(self, make_resource, make_rule):
        resources = [make_resource()]
        config = _default_config(**{STUB_PER_RESOURCE_ID: {"severity": "error"}})
        rule = make_rule(STUB_PER_RESOURCE_ID, fn=_stub_per_resource)

        result = evaluate(resources, [], config, rules=[rule])

        assert result.violations[0].severity == "error"

    def test_engine_returns_rule_output_unchanged(self, make_resource, make_rule):
        """Engine appends rule output verbatim; it does not rewrite Violation fields."""

        def prefilled_rule(resource, context):
            return Violation(
                rule_id="custom/already-set",
                resource_id=resource.resource_id,
                resource_name=resource.resource_name,
                message="pre-filled",
                severity="error",
                file_path=resource.file_path,
            )

        rule = make_rule(STUB_PER_RESOURCE_ID, fn=prefilled_rule)

        result = evaluate([make_resource()], [], _default_config(), rules=[rule])

        violation = result.violations[0]
        assert violation.rule_id == "custom/already-set"
        assert violation.severity == "error"


class TestEngineFailFast:
    """fail_fast stops evaluation after the first violation."""

    def test_per_resource_stops_at_one(self, make_resource, make_rule):
        resources = [make_resource() for _ in range(5)]
        rule = make_rule(STUB_PER_RESOURCE_ID, fn=_stub_per_resource)

        result = evaluate(
            resources, [], _default_config(), rules=[rule], fail_fast=True
        )

        assert len(result.violations) == 1

    def test_aggregate_stops_at_first_batch(self, make_resource, make_rule):
        resources = [make_resource(), make_resource()]
        rule = make_rule(STUB_AGGREGATE_ID, fn=_stub_aggregate, is_per_resource=False)
        second_rule = make_rule("test/should-not-run", fn=_stub_per_resource)

        result = evaluate(
            resources,
            [],
            _default_config(),
            rules=[rule, second_rule],
            fail_fast=True,
        )

        rule_ids = {v.rule_id for v in result.violations}
        assert rule_ids == {STUB_AGGREGATE_ID}

    def test_no_violation_means_no_early_stop(self, make_resource, make_rule):
        resources = [make_resource()]
        clean_rule = make_rule(STUB_PER_RESOURCE_ID, fn=_stub_per_resource_clean)
        violating_rule = make_rule("test/second", fn=_stub_per_resource)

        result = evaluate(
            resources,
            [],
            _default_config(),
            rules=[clean_rule, violating_rule],
            fail_fast=True,
        )

        assert len(result.violations) == 1
        assert result.violations[0].rule_id == "test/second"
