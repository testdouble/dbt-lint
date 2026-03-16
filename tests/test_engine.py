"""Tests for rule engine: dispatch, exclusion, filtering."""

import textwrap
from pathlib import Path

from dbt_linter.config import load_config
from dbt_linter.engine import EvaluationResult, evaluate

UNDOCUMENTED = "documentation/undocumented-models"


def _by_rule(result: EvaluationResult, rule_id: str):
    return [v for v in result.violations if v.rule_id == rule_id]


class TestEvaluate:
    def test_runs_per_resource_rules(self, make_resource):
        resources = [
            make_resource(
                resource_type="model",
                is_described=False,
                resource_name="undocumented",
            ),
            make_resource(
                resource_type="model",
                is_described=True,
            ),
        ]
        config = load_config(None)
        result = evaluate(resources, [], config)
        doc_violations = _by_rule(result, UNDOCUMENTED)
        assert len(doc_violations) == 1
        assert "undocumented" in doc_violations[0].message

    def test_runs_aggregate_rules(self, make_resource, make_relationship):
        src = make_resource(
            resource_id="source.pkg.raw.orders",
            resource_type="source",
            model_type="",
        )
        m1 = make_resource(
            resource_id="model.pkg.stg_a",
            resource_type="model",
        )
        m2 = make_resource(
            resource_id="model.pkg.stg_b",
            resource_type="model",
        )
        rels = [
            make_relationship(
                parent=src.resource_id,
                child=m1.resource_id,
                parent_resource_type="source",
                child_resource_type="model",
            ),
            make_relationship(
                parent=src.resource_id,
                child=m2.resource_id,
                parent_resource_type="source",
                child_resource_type="model",
            ),
        ]
        config = load_config(None)
        result = evaluate([src, m1, m2], rels, config)
        fanout = _by_rule(result, "modeling/source-fanout")
        assert len(fanout) == 1

    def test_disabled_rule_skipped(self, tmp_path: Path, make_resource):
        config_file = tmp_path / "dbt_linter.yml"
        config_file.write_text(
            textwrap.dedent("""\
            rules:
              documentation/undocumented-models:
                enabled: false
        """)
        )
        resources = [
            make_resource(resource_type="model", is_described=False),
        ]
        config = load_config(config_file)
        result = evaluate(resources, [], config)
        doc_violations = _by_rule(result, UNDOCUMENTED)
        assert len(doc_violations) == 0

    def test_meta_skip_excludes_resource(self, make_resource):
        resources = [
            make_resource(
                resource_type="model",
                is_described=False,
                skip_rules=frozenset(["documentation/undocumented-models"]),
            ),
        ]
        config = load_config(None)
        result = evaluate(resources, [], config)
        doc_violations = _by_rule(result, UNDOCUMENTED)
        assert len(doc_violations) == 0

    def test_exclude_resources_glob(self, tmp_path: Path, make_resource):
        config_file = tmp_path / "dbt_linter.yml"
        config_file.write_text(
            textwrap.dedent("""\
            rules:
              documentation/undocumented-models:
                exclude_resources:
                  - "model.pkg.legacy_*"
        """)
        )
        resources = [
            make_resource(
                resource_id="model.pkg.legacy_orders",
                resource_type="model",
                is_described=False,
            ),
            make_resource(
                resource_id="model.pkg.stg_orders",
                resource_type="model",
                is_described=False,
            ),
        ]
        config = load_config(config_file)
        result = evaluate(resources, [], config)
        doc_violations = _by_rule(result, UNDOCUMENTED)
        # legacy_orders excluded, stg_orders not
        assert len(doc_violations) == 1
        assert doc_violations[0].resource_id == "model.pkg.stg_orders"

    def test_fail_fast_stops_after_first_violation(self, make_resource):
        resources = [
            make_resource(
                resource_type="model",
                is_described=False,
                resource_name="undoc_a",
                resource_id="model.pkg.undoc_a",
            ),
            make_resource(
                resource_type="model",
                is_described=False,
                resource_name="undoc_b",
                resource_id="model.pkg.undoc_b",
            ),
        ]
        config = load_config(None)
        all_result = evaluate(resources, [], config)
        fast_result = evaluate(resources, [], config, fail_fast=True)
        assert len(fast_result.violations) < len(all_result.violations)
        assert len(fast_result.violations) >= 1

    def test_fail_fast_false_returns_all(self, make_resource):
        resources = [
            make_resource(
                resource_type="model",
                is_described=False,
                resource_name="undoc_a",
                resource_id="model.pkg.undoc_a",
            ),
            make_resource(
                resource_type="model",
                is_described=False,
                resource_name="undoc_b",
                resource_id="model.pkg.undoc_b",
            ),
        ]
        config = load_config(None)
        result = evaluate(resources, [], config, fail_fast=False)
        # Should find violations for both undescribed models
        doc_violations = _by_rule(result, UNDOCUMENTED)
        assert len(doc_violations) == 2

    def test_severity_override(self, tmp_path: Path, make_resource):
        config_file = tmp_path / "dbt_linter.yml"
        config_file.write_text(
            textwrap.dedent("""\
            rules:
              documentation/undocumented-models:
                severity: error
        """)
        )
        resources = [
            make_resource(resource_type="model", is_described=False),
        ]
        config = load_config(config_file)
        result = evaluate(resources, [], config)
        doc_violations = _by_rule(result, UNDOCUMENTED)
        assert len(doc_violations) == 1
        assert doc_violations[0].severity == "error"

    def test_exclude_resources_suppresses_aggregate_rule(
        self,
        tmp_path: Path,
        make_resource,
        make_relationship,
    ):
        """exclude_resources should suppress aggregate rule violations."""
        src = make_resource(
            resource_id="source.pkg.raw.orders",
            resource_type="source",
            model_type="",
        )
        m1 = make_resource(
            resource_id="model.pkg.stg_a",
            resource_type="model",
        )
        m2 = make_resource(
            resource_id="model.pkg.stg_b",
            resource_type="model",
        )
        rels = [
            make_relationship(
                parent=src.resource_id,
                child=m1.resource_id,
                parent_resource_type="source",
                child_resource_type="model",
            ),
            make_relationship(
                parent=src.resource_id,
                child=m2.resource_id,
                parent_resource_type="source",
                child_resource_type="model",
            ),
        ]
        # Exclude the source that fans out
        config_file = tmp_path / "dbt_linter.yml"
        config_file.write_text(
            textwrap.dedent("""\
            rules:
              modeling/source-fanout:
                exclude_resources:
                  - "source.pkg.raw.orders"
        """)
        )
        config = load_config(config_file)
        result = evaluate([src, m1, m2], rels, config)
        fanout = _by_rule(result, "modeling/source-fanout")
        assert len(fanout) == 0
        assert result.excluded >= 1
