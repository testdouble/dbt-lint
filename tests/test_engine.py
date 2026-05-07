"""Unit tests for rule engine: dispatch, exclusion, dispatch shape, fail-fast."""

from dbt_lint.config import DEFAULTS, Config
from dbt_lint.engine import evaluate
from dbt_lint.models import Violation
from dbt_lint.rules import RuleDef

# ---------------------------------------------------------------------------
# Stub helpers for isolated engine tests
# ---------------------------------------------------------------------------

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


def _make_rule(
    rule_id: str = STUB_PER_RESOURCE_ID,
    fn=_stub_per_resource,
    *,
    is_per_resource: bool = True,
) -> RuleDef:
    return RuleDef(
        id=rule_id,
        category=rule_id.split("/", maxsplit=1)[0],
        description=f"stub {rule_id}",
        fn=fn,
        is_per_resource=is_per_resource,
    )


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


# ---------------------------------------------------------------------------
# Isolated engine unit tests (stub rules only, no real rule registry)
# ---------------------------------------------------------------------------


class TestEngineDispatch:
    """Engine dispatch with injected stub rules."""

    def test_per_resource_dispatches_to_each_resource(self, make_resource):
        resources = [make_resource(), make_resource(), make_resource()]

        result = evaluate(resources, [], _default_config(), rules=[_make_rule()])

        assert len(result.violations) == 3

    def test_aggregate_dispatches_with_resources_and_relationships(
        self, make_resource, make_relationship
    ):
        resources = [make_resource(), make_resource()]
        rels = [make_relationship()]
        captured = {}

        def spy_aggregate(res, relationships, context):
            captured["resources"] = res
            captured["relationships"] = relationships
            return [context.violation(res[0], "agg")]

        rule = _make_rule(STUB_AGGREGATE_ID, spy_aggregate, is_per_resource=False)

        result = evaluate(resources, rels, _default_config(), rules=[rule])

        assert captured["resources"] == resources
        assert captured["relationships"] == rels
        assert len(result.violations) == 1


class TestEngineExclusion:
    """Engine exclusion: disabled, meta_skip, exclude_resources, path filters."""

    def test_disabled_rule_produces_no_violations(self, make_resource):
        resources = [make_resource()]
        config = _default_config(**{STUB_PER_RESOURCE_ID: {"enabled": False}})

        result = evaluate(resources, [], config, rules=[_make_rule()])

        assert not result.violations

    def test_meta_skip_excludes_resource(self, make_resource):
        skipped = make_resource(skip_rules=frozenset([STUB_PER_RESOURCE_ID]))
        included = make_resource()

        result = evaluate(
            [skipped, included], [], _default_config(), rules=[_make_rule()]
        )

        assert len(result.violations) == 1
        assert result.violations[0].resource_id == included.resource_id

    def test_exclude_resources_glob_filters_per_resource(self, make_resource):
        legacy = make_resource(resource_id="model.pkg.legacy_orders")
        current = make_resource(resource_id="model.pkg.stg_orders")
        config = _default_config(
            **{STUB_PER_RESOURCE_ID: {"exclude_resources": ["model.pkg.legacy_*"]}}
        )

        result = evaluate([legacy, current], [], config, rules=[_make_rule()])

        assert len(result.violations) == 1
        assert result.violations[0].resource_id == "model.pkg.stg_orders"

    def test_exclude_resources_glob_filters_aggregate(self, make_resource):
        legacy = make_resource(resource_id="model.pkg.legacy_a")
        current = make_resource(resource_id="model.pkg.stg_a")
        config = _default_config(
            **{STUB_AGGREGATE_ID: {"exclude_resources": ["model.pkg.legacy_*"]}}
        )
        rule = _make_rule(STUB_AGGREGATE_ID, _stub_aggregate, is_per_resource=False)

        result = evaluate([legacy, current], [], config, rules=[rule])

        # legacy_a excluded at eligibility check; only stg_a passed to rule
        assert len(result.violations) == 1
        assert result.violations[0].resource_id == "model.pkg.stg_a"

    def test_include_path_filter(self, make_resource):
        inside = make_resource(file_path="models/staging/orders.sql")
        outside = make_resource(file_path="models/marts/orders.sql")
        config = Config(
            params={},
            include="models/staging/.*",
            exclude=None,
            config_dir=None,
            _rule_overrides={},
            _custom_rule_entries=[],
        )

        result = evaluate([inside, outside], [], config, rules=[_make_rule()])

        assert len(result.violations) == 1
        assert result.violations[0].resource_id == inside.resource_id


class TestEngineContextPlumbing:
    """Engine constructs the RuleContext that rules use to build violations."""

    def test_context_carries_rule_id_and_default_severity(self, make_resource):
        resources = [make_resource()]

        result = evaluate(resources, [], _default_config(), rules=[_make_rule()])

        violation = result.violations[0]
        assert violation.rule_id == STUB_PER_RESOURCE_ID
        assert violation.severity == "warn"

    def test_severity_override_from_config(self, make_resource):
        resources = [make_resource()]
        config = _default_config(**{STUB_PER_RESOURCE_ID: {"severity": "error"}})

        result = evaluate(resources, [], config, rules=[_make_rule()])

        assert result.violations[0].severity == "error"

    def test_engine_returns_rule_output_unchanged(self, make_resource):
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

        rule = _make_rule(fn=prefilled_rule)

        result = evaluate([make_resource()], [], _default_config(), rules=[rule])

        violation = result.violations[0]
        assert violation.rule_id == "custom/already-set"
        assert violation.severity == "error"


class TestEngineFailFast:
    """fail_fast stops evaluation after the first violation."""

    def test_per_resource_stops_at_one(self, make_resource):
        resources = [make_resource() for _ in range(5)]

        result = evaluate(
            resources, [], _default_config(), rules=[_make_rule()], fail_fast=True
        )

        assert len(result.violations) == 1

    def test_aggregate_stops_at_first_batch(self, make_resource):
        resources = [make_resource(), make_resource()]
        rule = _make_rule(STUB_AGGREGATE_ID, _stub_aggregate, is_per_resource=False)
        # Add a second rule that should never run
        second_rule = _make_rule(
            "test/should-not-run", _stub_per_resource, is_per_resource=True
        )

        result = evaluate(
            resources,
            [],
            _default_config(),
            rules=[rule, second_rule],
            fail_fast=True,
        )

        # Only the aggregate rule's violations; second rule never fires
        rule_ids = {v.rule_id for v in result.violations}
        assert rule_ids == {STUB_AGGREGATE_ID}

    def test_no_violation_means_no_early_stop(self, make_resource):
        resources = [make_resource()]
        clean_rule = _make_rule(fn=_stub_per_resource_clean)
        violating_rule = _make_rule(rule_id="test/second", fn=_stub_per_resource)

        result = evaluate(
            resources,
            [],
            _default_config(),
            rules=[clean_rule, violating_rule],
            fail_fast=True,
        )

        assert len(result.violations) == 1
        assert result.violations[0].rule_id == "test/second"
