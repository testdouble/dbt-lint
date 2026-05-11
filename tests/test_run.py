"""Tests for the run() facade in dbt_lint._lint."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar

import pytest
import yaml

from dbt_lint._lint import (
    ConfigError,
    CustomRuleError,
    LintError,
    LintResult,
    ManifestError,
    run,
)
from dbt_lint.config import Config, CustomRuleEntry
from dbt_lint.engine import EvaluationResult
from dbt_lint.models import DirectEdge, Violation
from dbt_lint.rules import RuleDef
from helpers import fixture_manifest_dict


def _empty_config(custom_entries: list[CustomRuleEntry] | None = None) -> Config:
    return Config(
        params={},
        include=None,
        exclude=None,
        config_dir=None,
        _rule_overrides={},
        _custom_rule_entries=custom_entries or [],
    )


def _violation(rule_id: str = "documentation/undocumented-models") -> Violation:
    return Violation(
        rule_id=rule_id,
        resource_id="model.pkg.example",
        resource_name="example",
        message="example violation",
        severity="warn",
        file_path="models/example.sql",
        patch_path="",
    )


class FakeRegistry:
    instances: ClassVar[list[FakeRegistry]] = []

    def __init__(self) -> None:
        self.registered: list[tuple[str, str, Path]] = []
        self._rules: list[RuleDef] = []
        FakeRegistry.instances.append(self)

    def register_from_path(self, path: str, rule_id: str, config_dir: Path) -> None:
        self.registered.append((path, rule_id, config_dir))

    def all(self) -> list[RuleDef]:
        return list(self._rules)


@pytest.fixture(autouse=True)
def _reset_fake_registry():
    FakeRegistry.instances = []
    yield
    FakeRegistry.instances = []


@pytest.fixture
def pipeline_doubles(monkeypatch):
    """Install pass-through doubles for run()'s pipeline deps; return a call recorder."""
    config = _empty_config()
    record: dict[str, Any] = {
        "load_config": [],
        "load_suppressions": [],
        "merge_suppressions": [],
        "parse_manifest": [],
        "build_relationships": [],
        "evaluate": [],
        "config_returned": config,
    }

    def stub_load_config(path=None, **_kwargs):
        record["load_config"].append(path)
        return config

    def stub_load_suppressions(path):
        record["load_suppressions"].append(path)
        return {"some-rule": {"enabled": False}}

    def stub_merge_suppressions(config, suppressions_rules):
        record["merge_suppressions"].append((config, suppressions_rules))
        return config

    def stub_parse_manifest(path, config):
        record["parse_manifest"].append((path, config))
        return [], []

    def stub_build_relationships(resources, edges):
        record["build_relationships"].append((resources, edges))
        return []

    def stub_evaluate(resources, relationships, config, *, rules, fail_fast=False):
        record["evaluate"].append(
            {
                "resources": resources,
                "relationships": relationships,
                "config": config,
                "rules": rules,
                "fail_fast": fail_fast,
            }
        )
        return EvaluationResult(violations=[], excluded=0)

    monkeypatch.setattr("dbt_lint._lint.load_config", stub_load_config)
    monkeypatch.setattr("dbt_lint._lint.load_suppressions", stub_load_suppressions)
    monkeypatch.setattr("dbt_lint._lint.merge_suppressions", stub_merge_suppressions)
    monkeypatch.setattr("dbt_lint._lint.parse_manifest", stub_parse_manifest)
    monkeypatch.setattr("dbt_lint._lint.build_relationships", stub_build_relationships)
    monkeypatch.setattr("dbt_lint._lint.evaluate", stub_evaluate)
    monkeypatch.setattr("dbt_lint._lint.Registry", FakeRegistry)

    return record


class TestPipelineThreading:
    def test_loads_config_from_config_path(self, pipeline_doubles, tmp_path):
        config_path = tmp_path / "dbt-lint.yml"

        subject = run

        subject(
            manifest_path=tmp_path / "manifest.json",
            config_path=config_path,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        assert pipeline_doubles["load_config"] == [config_path]

    def test_passes_loaded_config_to_parse_manifest(self, pipeline_doubles, tmp_path):
        manifest_path = tmp_path / "manifest.json"

        subject = run

        subject(
            manifest_path=manifest_path,
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        parse_args = pipeline_doubles["parse_manifest"][0]
        assert parse_args[0] == manifest_path
        assert parse_args[1] is pipeline_doubles["config_returned"]

    def test_chains_resources_and_edges_to_build_relationships(
        self, pipeline_doubles, monkeypatch, make_resource, tmp_path
    ):
        resource = make_resource()
        edge = DirectEdge(parent="model.pkg.a", child="model.pkg.b")

        def stub_parse_manifest(path, config):
            return [resource], [edge]

        monkeypatch.setattr("dbt_lint._lint.parse_manifest", stub_parse_manifest)

        subject = run

        subject(
            manifest_path=tmp_path / "manifest.json",
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        build_args = pipeline_doubles["build_relationships"][0]
        assert build_args == ([resource], [edge])

    def test_chains_relationships_into_evaluate(
        self, pipeline_doubles, monkeypatch, make_relationship, tmp_path
    ):
        relationship = make_relationship()

        def stub_build_relationships(resources, edges):
            return [relationship]

        monkeypatch.setattr(
            "dbt_lint._lint.build_relationships", stub_build_relationships
        )

        subject = run

        subject(
            manifest_path=tmp_path / "manifest.json",
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        evaluate_call = pipeline_doubles["evaluate"][0]
        assert evaluate_call["relationships"] == [relationship]
        assert evaluate_call["config"] is pipeline_doubles["config_returned"]


class TestLintResultShape:
    def test_violations_come_from_engine_evaluation(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        violation = _violation()

        def stub_evaluate(resources, relationships, config, *, rules, fail_fast=False):
            return EvaluationResult(violations=[violation], excluded=0)

        monkeypatch.setattr("dbt_lint._lint.evaluate", stub_evaluate)

        subject = run

        result = subject(
            manifest_path=tmp_path / "manifest.json",
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        assert isinstance(result, LintResult)
        assert result.violations == [violation]

    def test_excluded_count_comes_from_engine(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        def stub_evaluate(resources, relationships, config, *, rules, fail_fast=False):
            return EvaluationResult(violations=[], excluded=7)

        monkeypatch.setattr("dbt_lint._lint.evaluate", stub_evaluate)

        subject = run

        result = subject(
            manifest_path=tmp_path / "manifest.json",
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        assert result.excluded == 7

    def test_resource_counts_aggregates_by_resource_type(
        self, pipeline_doubles, monkeypatch, make_resource, tmp_path
    ):
        resources = [
            make_resource(resource_type="model"),
            make_resource(resource_type="model"),
            make_resource(resource_type="source"),
            make_resource(resource_type="exposure"),
        ]

        def stub_parse_manifest(path, config):
            return resources, []

        monkeypatch.setattr("dbt_lint._lint.parse_manifest", stub_parse_manifest)

        subject = run

        result = subject(
            manifest_path=tmp_path / "manifest.json",
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        assert result.resource_counts == {"model": 2, "source": 1, "exposure": 1}


class TestSuppressions:
    def test_no_suppressions_path_skips_merge(self, pipeline_doubles, tmp_path):
        subject = run

        subject(
            manifest_path=tmp_path / "manifest.json",
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        assert pipeline_doubles["load_suppressions"] == []
        assert pipeline_doubles["merge_suppressions"] == []

    def test_explicit_suppressions_path_loads_and_merges(
        self, pipeline_doubles, tmp_path
    ):
        suppressions_path = tmp_path / ".dbt-lint-suppressions.yml"

        subject = run

        subject(
            manifest_path=tmp_path / "manifest.json",
            config_path=None,
            suppressions_path=suppressions_path,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        assert pipeline_doubles["load_suppressions"] == [suppressions_path]
        assert len(pipeline_doubles["merge_suppressions"]) == 1
        merged_config, merged_rules = pipeline_doubles["merge_suppressions"][0]
        assert merged_config is pipeline_doubles["config_returned"]
        assert merged_rules == {"some-rule": {"enabled": False}}


def _seed_registry(monkeypatch, rules: list[RuleDef]) -> None:
    """Replace ``Registry`` so ``collect_rules`` returns the supplied rules."""

    class SeededRegistry(FakeRegistry):
        def __init__(self) -> None:
            super().__init__()
            self._rules = list(rules)

    monkeypatch.setattr("dbt_lint._lint.Registry", SeededRegistry)


class TestRuleIdFilters:
    """run() filters the rule list pre-evaluation so --fail-fast respects
    --select/--exclude. Filter semantics are unit-tested in test_filters.py."""

    def test_select_narrows_rules_passed_to_evaluate(
        self, pipeline_doubles, monkeypatch, make_rule, tmp_path
    ):
        kept = make_rule("a/keep")
        dropped = make_rule("b/drop")
        _seed_registry(monkeypatch, [kept, dropped])

        run(
            manifest_path=tmp_path / "manifest.json",
            config_path=None,
            suppressions_path=None,
            select=("a/keep",),
            exclude=(),
            fail_fast=False,
        )

        eval_rules = pipeline_doubles["evaluate"][0]["rules"]
        assert [rule.id for rule in eval_rules] == ["a/keep"]

    def test_exclude_drops_rules_passed_to_evaluate(
        self, pipeline_doubles, monkeypatch, make_rule, tmp_path
    ):
        kept = make_rule("a/keep")
        dropped = make_rule("b/drop")
        _seed_registry(monkeypatch, [kept, dropped])

        run(
            manifest_path=tmp_path / "manifest.json",
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=("b/drop",),
            fail_fast=False,
        )

        eval_rules = pipeline_doubles["evaluate"][0]["rules"]
        assert [rule.id for rule in eval_rules] == ["a/keep"]


class TestFailFast:
    def test_fail_fast_passed_to_evaluate(self, pipeline_doubles, tmp_path):
        subject = run

        subject(
            manifest_path=tmp_path / "manifest.json",
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=True,
        )

        assert pipeline_doubles["evaluate"][0]["fail_fast"] is True


class TestCustomRuleAssembly:
    def test_custom_entries_registered_via_registry(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        config_dir = tmp_path
        config = _empty_config(
            custom_entries=[
                CustomRuleEntry(
                    rule_id="custom/example",
                    source="rules/example.py",
                    overrides={},
                )
            ]
        )
        config.config_dir = config_dir

        def stub_load_config(path=None, **_kwargs):
            return config

        monkeypatch.setattr("dbt_lint._lint.load_config", stub_load_config)

        subject = run

        subject(
            manifest_path=tmp_path / "manifest.json",
            config_path=tmp_path / "dbt-lint.yml",
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        assert len(FakeRegistry.instances) == 1
        registry = FakeRegistry.instances[0]
        assert registry.registered == [
            ("rules/example.py", "custom/example", config_dir)
        ]

    def test_no_custom_entries_skips_registration(self, pipeline_doubles, tmp_path):
        subject = run

        subject(
            manifest_path=tmp_path / "manifest.json",
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        assert len(FakeRegistry.instances) == 1
        assert FakeRegistry.instances[0].registered == []


class TestFixtureManifest:
    """SAFE integration: real fixture manifest through the assembled pipeline."""

    def test_returns_lint_result_with_known_violations(self, tmp_path):
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(fixture_manifest_dict()))

        subject = run

        result = subject(
            manifest_path=manifest_path,
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=False,
        )

        rule_ids = {violation.rule_id for violation in result.violations}
        assert "documentation/undocumented-models" in rule_ids
        assert "governance/public-models-without-contract" in rule_ids
        assert "governance/undocumented-public-models" in rule_ids
        assert result.resource_counts == {"model": 2, "source": 1, "exposure": 1}

    def test_exclude_first_firing_rule_with_fail_fast_still_returns_violations(
        self, tmp_path
    ):
        """Regression: rule-ID exclusion is applied pre-evaluation. Otherwise
        ``fail_fast`` returns after the excluded rule fires, then the
        post-evaluation filter strips that violation, masking every other
        rule's output."""
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(fixture_manifest_dict()))

        baseline = run(
            manifest_path=manifest_path,
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(),
            fail_fast=True,
        )
        assert baseline.violations, "fixture should produce at least one violation"
        first_firing_rule = baseline.violations[0].rule_id

        result = run(
            manifest_path=manifest_path,
            config_path=None,
            suppressions_path=None,
            select=(),
            exclude=(first_firing_rule,),
            fail_fast=True,
        )

        assert result.violations, (
            f"expected a violation from a different rule after excluding "
            f"{first_firing_rule}; got none"
        )
        assert first_firing_rule not in {v.rule_id for v in result.violations}


def _run_with_defaults(
    manifest_path,
    *,
    config_path=None,
    suppressions_path=None,
):
    return run(
        manifest_path=manifest_path,
        config_path=config_path,
        suppressions_path=suppressions_path,
        select=(),
        exclude=(),
        fail_fast=False,
    )


class TestLintErrorHierarchy:
    def test_subtypes_extend_lint_error(self):
        assert issubclass(ConfigError, LintError)
        assert issubclass(ManifestError, LintError)
        assert issubclass(CustomRuleError, LintError)

    def test_lint_error_extends_exception(self):
        assert issubclass(LintError, Exception)


class TestConfigErrors:
    def test_yaml_parse_failure_raises_config_error(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        def stub_load_config(path=None, **_kwargs):
            raise yaml.YAMLError("invalid yaml syntax")

        monkeypatch.setattr("dbt_lint._lint.load_config", stub_load_config)

        with pytest.raises(ConfigError, match="invalid yaml syntax"):
            _run_with_defaults(tmp_path / "manifest.json")

    def test_config_file_io_failure_raises_config_error(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        def stub_load_config(path=None, **_kwargs):
            raise FileNotFoundError("config missing")

        monkeypatch.setattr("dbt_lint._lint.load_config", stub_load_config)

        with pytest.raises(ConfigError, match="config missing"):
            _run_with_defaults(tmp_path / "manifest.json")

    def test_suppressions_load_failure_raises_config_error(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        def stub_load_suppressions(path):
            raise yaml.YAMLError("bad suppressions")

        monkeypatch.setattr("dbt_lint._lint.load_suppressions", stub_load_suppressions)

        with pytest.raises(ConfigError, match="bad suppressions"):
            _run_with_defaults(
                tmp_path / "manifest.json",
                suppressions_path=tmp_path / ".dbt-lint-suppressions.yml",
            )


class TestManifestErrors:
    def test_json_decode_failure_raises_manifest_error(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        def stub_parse_manifest(path, config):
            raise json.JSONDecodeError("invalid json", "doc", 0)

        monkeypatch.setattr("dbt_lint._lint.parse_manifest", stub_parse_manifest)

        with pytest.raises(ManifestError, match="invalid json"):
            _run_with_defaults(tmp_path / "manifest.json")

    def test_schema_mismatch_raises_manifest_error(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        def stub_parse_manifest(path, config):
            raise KeyError("nodes")

        monkeypatch.setattr("dbt_lint._lint.parse_manifest", stub_parse_manifest)

        with pytest.raises(ManifestError, match="nodes"):
            _run_with_defaults(tmp_path / "manifest.json")

    def test_manifest_file_io_failure_raises_manifest_error(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        def stub_parse_manifest(path, config):
            raise FileNotFoundError("manifest missing")

        monkeypatch.setattr("dbt_lint._lint.parse_manifest", stub_parse_manifest)

        with pytest.raises(ManifestError, match="manifest missing"):
            _run_with_defaults(tmp_path / "manifest.json")


class TestCustomRuleErrors:
    def test_registry_import_failure_raises_custom_rule_error(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        config = _empty_config(
            custom_entries=[
                CustomRuleEntry(
                    rule_id="custom/example",
                    source="rules/example.py",
                    overrides={},
                )
            ]
        )
        config.config_dir = tmp_path

        def stub_load_config(path=None, **_kwargs):
            return config

        monkeypatch.setattr("dbt_lint._lint.load_config", stub_load_config)

        class FailingRegistry(FakeRegistry):
            def register_from_path(self, path, rule_id, config_dir):
                raise ImportError("module load failed")

        monkeypatch.setattr("dbt_lint._lint.Registry", FailingRegistry)

        with pytest.raises(CustomRuleError, match="module load failed"):
            _run_with_defaults(
                tmp_path / "manifest.json",
                config_path=tmp_path / "dbt-lint.yml",
            )

    def test_registry_validation_failure_raises_custom_rule_error(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        config = _empty_config(
            custom_entries=[
                CustomRuleEntry(
                    rule_id="custom/example",
                    source="rules/example.py",
                    overrides={},
                )
            ]
        )
        config.config_dir = tmp_path

        def stub_load_config(path=None, **_kwargs):
            return config

        monkeypatch.setattr("dbt_lint._lint.load_config", stub_load_config)

        class FailingRegistry(FakeRegistry):
            def register_from_path(self, path, rule_id, config_dir):
                raise ValueError("conflicts with built-in")

        monkeypatch.setattr("dbt_lint._lint.Registry", FailingRegistry)

        with pytest.raises(CustomRuleError, match="conflicts with built-in"):
            _run_with_defaults(
                tmp_path / "manifest.json",
                config_path=tmp_path / "dbt-lint.yml",
            )

    def test_custom_entries_without_config_dir_raises_custom_rule_error(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        config = _empty_config(
            custom_entries=[
                CustomRuleEntry(
                    rule_id="custom/example",
                    source="rules/example.py",
                    overrides={},
                )
            ]
        )

        def stub_load_config(path=None, **_kwargs):
            return config

        monkeypatch.setattr("dbt_lint._lint.load_config", stub_load_config)

        with pytest.raises(CustomRuleError, match="config file"):
            _run_with_defaults(tmp_path / "manifest.json")


class TestUnexpectedExceptionPropagates:
    def test_engine_evaluation_error_does_not_become_lint_error(
        self, pipeline_doubles, monkeypatch, tmp_path
    ):
        def stub_evaluate(resources, relationships, config, *, rules, fail_fast=False):
            raise RuntimeError("rule body bug")

        monkeypatch.setattr("dbt_lint._lint.evaluate", stub_evaluate)

        with pytest.raises(RuntimeError, match="rule body bug"):
            _run_with_defaults(tmp_path / "manifest.json")
