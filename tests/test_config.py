"""Tests for config loading, defaults, merging, RuleConfig, and path filtering."""

import textwrap
from pathlib import Path

import pytest

from dbt_lint.config import (
    DEFAULTS,
    load_config,
    load_suppressions,
    matches_path_filter,
    merge_suppressions,
)


class TestDefaults:
    def test_defaults_has_thresholds(self):
        assert DEFAULTS["models_fanout_threshold"] == 3
        assert DEFAULTS["too_many_joins_threshold"] == 5
        assert DEFAULTS["chained_views_threshold"] == 5
        assert DEFAULTS["documentation_coverage_target"] == 100
        assert DEFAULTS["test_coverage_target"] == 100

    def test_defaults_has_prefixes(self):
        assert DEFAULTS["staging_prefixes"] == ["stg_"]
        assert not DEFAULTS["marts_prefixes"]

    def test_defaults_has_materializations(self):
        assert DEFAULTS["staging_allowed_materializations"] == ["view"]
        assert "table" in DEFAULTS["marts_allowed_materializations"]

    def test_defaults_has_pk_test_macros(self):
        macros = DEFAULTS["primary_key_test_macros"]
        assert ["dbt.test_unique", "dbt.test_not_null"] in macros

    def test_defaults_include_exclude_are_none(self):
        assert DEFAULTS["include"] is None
        assert DEFAULTS["exclude"] is None

    def test_defaults_rules_is_empty(self):
        assert not DEFAULTS["rules"]


class TestLoadConfig:
    def test_load_defaults_when_no_file(self):
        config = load_config(None)
        assert (
            config.params["models_fanout_threshold"]
            == DEFAULTS["models_fanout_threshold"]
        )

    def test_load_from_yaml(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text(
            textwrap.dedent("""\
            models_fanout_threshold: 5
            too_many_joins_threshold: 10
        """)
        )
        config = load_config(config_file)
        assert config.params["models_fanout_threshold"] == 5
        assert config.params["too_many_joins_threshold"] == 10
        assert (
            config.params["chained_views_threshold"]
            == DEFAULTS["chained_views_threshold"]
        )

    def test_load_with_rule_overrides(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text(
            textwrap.dedent("""\
            rules:
              modeling/too-many-joins:
                severity: error
              structure/intermediate-materialization:
                enabled: false
        """)
        )
        config = load_config(config_file)
        rc = config.rule_config("modeling/too-many-joins")
        assert rc.severity == "error"
        assert rc.enabled is True

        rc2 = config.rule_config("structure/intermediate-materialization")
        assert rc2.enabled is False

    def test_load_with_exclude_resources(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text(
            textwrap.dedent("""\
            rules:
              testing/sources-without-freshness:
                exclude_resources:
                  - source.pkg.raw.legacy_*
        """)
        )
        config = load_config(config_file)
        rc = config.rule_config("testing/sources-without-freshness")
        assert "source.pkg.raw.legacy_*" in rc.exclude_resources

    def test_empty_yaml_file(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text("")
        config = load_config(config_file)
        assert (
            config.params["models_fanout_threshold"]
            == DEFAULTS["models_fanout_threshold"]
        )


class TestRuleConfig:
    def test_default_rule_config(self):
        config = load_config(None)
        rc = config.rule_config("modeling/source-fanout")
        assert rc.enabled is True
        assert rc.severity == "warn"
        assert not rc.exclude_resources
        assert rc.params is config.params

    def test_severity_override(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text(
            textwrap.dedent("""\
            rules:
              modeling/source-fanout:
                severity: error
        """)
        )
        config = load_config(config_file)
        rc = config.rule_config("modeling/source-fanout")
        assert rc.severity == "error"

    def test_unknown_rule_gets_defaults(self):
        config = load_config(None)
        rc = config.rule_config("nonexistent/rule")
        assert rc.enabled is True
        assert rc.severity == "warn"


class TestMergeSuppressions:
    def test_empty_suppressions_returns_same_config(self):
        config = load_config(None)
        merged = merge_suppressions(config, {})
        assert merged._rule_overrides == config._rule_overrides

    def test_adds_exclude_resources_to_existing_rule(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text(
            textwrap.dedent("""\
            rules:
              my-rule:
                exclude_resources:
                  - model.pkg.a
                  - model.pkg.b
        """)
        )
        config = load_config(config_file)
        suppressions_rules = {
            "my-rule": {"exclude_resources": ["model.pkg.b", "model.pkg.c"]},
        }
        merged = merge_suppressions(config, suppressions_rules)
        rc = merged.rule_config("my-rule")
        assert rc.exclude_resources == ["model.pkg.a", "model.pkg.b", "model.pkg.c"]

    def test_adds_new_rule_from_suppressions(self):
        config = load_config(None)
        suppressions_rules = {
            "new-rule": {"exclude_resources": ["model.pkg.d"]},
        }
        merged = merge_suppressions(config, suppressions_rules)
        rc = merged.rule_config("new-rule")
        assert rc.exclude_resources == ["model.pkg.d"]

    def test_enabled_false_overrides(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text(
            textwrap.dedent("""\
            rules:
              my-rule:
                severity: error
        """)
        )
        config = load_config(config_file)
        suppressions_rules = {"my-rule": {"enabled": False}}
        merged = merge_suppressions(config, suppressions_rules)
        rc = merged.rule_config("my-rule")
        assert rc.enabled is False
        assert rc.severity == "error"

    def test_preserves_main_severity(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text(
            textwrap.dedent("""\
            rules:
              my-rule:
                severity: error
        """)
        )
        config = load_config(config_file)
        suppressions_rules = {
            "my-rule": {"exclude_resources": ["model.pkg.a"]},
        }
        merged = merge_suppressions(config, suppressions_rules)
        rc = merged.rule_config("my-rule")
        assert rc.severity == "error"

    def test_does_not_mutate_original(self):
        config = load_config(None)
        original_overrides = dict(config._rule_overrides)
        merge_suppressions(config, {"new-rule": {"enabled": False}})
        assert config._rule_overrides == original_overrides

    def test_preserves_custom_rule_entries(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text(
            textwrap.dedent("""\
            rules:
              custom/my-rule:
                source: my_rules.py
        """)
        )
        config = load_config(config_file)
        suppressions_rules = {
            "custom/my-rule": {"exclude_resources": ["model.pkg.a"]},
        }
        merged = merge_suppressions(config, suppressions_rules)
        assert len(merged._custom_rule_entries) == 1
        rc = merged.rule_config("custom/my-rule")
        assert rc.exclude_resources == ["model.pkg.a"]


class TestLoadSuppressions:
    def test_valid_file(self, tmp_path: Path):
        suppressions = tmp_path / "suppressions.yml"
        suppressions.write_text(
            textwrap.dedent("""\
            rules:
              documentation/undocumented-models:
                exclude_resources:
                  - model.pkg.stg_users
              documentation/documentation-coverage:
                enabled: false
        """)
        )
        result = load_suppressions(suppressions)
        assert result["documentation/undocumented-models"] == {
            "exclude_resources": ["model.pkg.stg_users"],
        }
        assert result["documentation/documentation-coverage"] == {"enabled": False}

    def test_empty_file(self, tmp_path: Path):
        suppressions = tmp_path / "suppressions.yml"
        suppressions.write_text("")
        assert not load_suppressions(suppressions)

    def test_strips_non_allowed_keys(self, tmp_path: Path):
        suppressions = tmp_path / "suppressions.yml"
        suppressions.write_text(
            textwrap.dedent("""\
            rules:
              my-rule:
                severity: error
                source: custom.py
                exclude_resources:
                  - model.pkg.foo
        """)
        )
        result = load_suppressions(suppressions)
        assert result["my-rule"] == {"exclude_resources": ["model.pkg.foo"]}

    def test_skips_non_dict_entries(self, tmp_path: Path):
        suppressions = tmp_path / "suppressions.yml"
        suppressions.write_text(
            textwrap.dedent("""\
            rules:
              bad-rule: true
              good-rule:
                exclude_resources:
                  - model.pkg.foo
        """)
        )
        result = load_suppressions(suppressions)
        assert "bad-rule" not in result
        assert "good-rule" in result


class TestMatchesPathFilter:
    def test_no_filters(self):
        assert matches_path_filter("models/staging/stg_orders.sql", None, None) is True

    def test_include_matches(self):
        assert (
            matches_path_filter("models/staging/stg_orders.sql", "models/staging", None)
            is True
        )

    def test_include_no_match(self):
        assert (
            matches_path_filter("models/marts/fct_orders.sql", "models/staging", None)
            is False
        )

    def test_exclude_matches(self):
        assert (
            matches_path_filter(
                "models/staging/stg_legacy.sql", None, "models/staging/stg_legacy"
            )
            is False
        )

    def test_exclude_no_match(self):
        assert (
            matches_path_filter("models/staging/stg_orders.sql", None, "models/marts")
            is True
        )

    def test_include_and_exclude(self):
        assert (
            matches_path_filter(
                "models/staging/stg_legacy.sql",
                "models/staging",
                "models/staging/stg_legacy",
            )
            is False
        )

    def test_regex_pattern(self):
        pattern = r"models/staging/stg_\w+\.sql"
        assert (
            matches_path_filter("models/staging/stg_orders.sql", pattern, None) is True
        )
        assert (
            matches_path_filter("models/staging/raw_orders.sql", pattern, None) is False
        )


class TestConfigRegexValidation:
    def test_invalid_include_rejected_at_load(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text("include: '[invalid'\n")
        with pytest.raises(ValueError, match="Invalid regex"):
            load_config(config_file)

    def test_invalid_exclude_rejected_at_load(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text("exclude: '[invalid'\n")
        with pytest.raises(ValueError, match="Invalid regex"):
            load_config(config_file)

    def test_valid_regex_accepted(self, tmp_path: Path):
        config_file = tmp_path / "dbt_lint.yml"
        config_file.write_text(r"include: 'models/staging/stg_\w+\.sql'" + "\n")
        config = load_config(config_file)
        assert config.include == r"models/staging/stg_\w+\.sql"
