"""Tests for custom rule loading via source directive."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from dbt_lint.config import DEFAULTS, Config, CustomRuleEntry, load_config
from dbt_lint.loader import (
    _synthetic_module_name,
    load_custom_rules,
)


def _entry(rule_id: str, source: str) -> CustomRuleEntry:
    return CustomRuleEntry(rule_id=rule_id, source=source, overrides={})


def _write_rule_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content))


def _config_with_custom(tmp_path: Path, entries: list[CustomRuleEntry]) -> Config:
    """Build a Config with custom rule entries rooted at tmp_path."""
    return Config(
        params={**DEFAULTS},
        include=None,
        exclude=None,
        config_dir=tmp_path,
        _rule_overrides={e.rule_id: e.overrides for e in entries},
        _custom_rule_entries=entries,
    )


class TestSyntheticModuleName:
    def test_relative_path(self, tmp_path):
        source = tmp_path / "custom_rules" / "modeling" / "select_distinct.py"
        name = _synthetic_module_name(source, tmp_path)
        assert name == "dbt_lint_custom.custom_rules.modeling.select_distinct"

    def test_flat_file(self, tmp_path):
        source = tmp_path / "my_rule.py"
        name = _synthetic_module_name(source, tmp_path)
        assert name == "dbt_lint_custom.my_rule"


class TestLoadCustomRulesPerResource:
    def test_loads_per_resource_rule(self, tmp_path):
        rule_file = tmp_path / "custom_rules" / "flag_all.py"
        _write_rule_file(
            rule_file,
            """\
            from dbt_lint.extend import Resource, RuleConfig, Violation, rule

            @rule(id="custom/flag-all", description="Flags everything.")
            def flag_all(resource: Resource, config: RuleConfig) -> Violation | None:
                return Violation.from_resource(resource, "flagged")
            """,
        )

        config = _config_with_custom(
            tmp_path,
            [_entry("custom/flag-all", "custom_rules/flag_all.py")],
        )
        rules = load_custom_rules(config)
        assert len(rules) == 1
        assert rules[0].id == "custom/flag-all"
        assert rules[0].is_per_resource is True

    def test_loads_aggregate_rule(self, tmp_path):
        rule_file = tmp_path / "agg.py"
        _write_rule_file(
            rule_file,
            """\
            from dbt_lint.extend import (
                Resource, Relationship, RuleConfig, Violation, rule,
            )

            @rule(id="custom/agg", description="Aggregate.")
            def agg(
                resources: list[Resource],
                relationships: list[Relationship],
                config: RuleConfig,
            ) -> list[Violation]:
                return []
            """,
        )

        config = _config_with_custom(
            tmp_path,
            [_entry("custom/agg", "agg.py")],
        )
        rules = load_custom_rules(config)
        assert len(rules) == 1
        assert rules[0].is_per_resource is False


class TestIdempotence:
    def test_same_file_imported_once(self, tmp_path):
        rule_file = tmp_path / "multi.py"
        _write_rule_file(
            rule_file,
            """\
            from dbt_lint.extend import Resource, RuleConfig, Violation, rule

            @rule(id="custom/rule-a", description="Rule A.")
            def rule_a(resource: Resource, config: RuleConfig) -> Violation | None:
                return None

            @rule(id="custom/rule-b", description="Rule B.")
            def rule_b(resource: Resource, config: RuleConfig) -> Violation | None:
                return None
            """,
        )

        config = _config_with_custom(
            tmp_path,
            [
                _entry("custom/rule-a", "multi.py"),
                _entry("custom/rule-b", "multi.py"),
            ],
        )

        # Clean up any prior import
        mod_name = "dbt_lint_custom.multi"
        sys.modules.pop(mod_name, None)

        rules = load_custom_rules(config)
        assert len(rules) == 2
        ids = {r.id for r in rules}
        assert ids == {"custom/rule-a", "custom/rule-b"}


class TestValidationErrors:
    def test_file_not_found(self, tmp_path):
        config = _config_with_custom(
            tmp_path,
            [_entry("custom/missing", "nonexistent.py")],
        )
        with pytest.raises(FileNotFoundError, match="file not found"):
            load_custom_rules(config)

    def test_no_rule_function(self, tmp_path):
        rule_file = tmp_path / "empty.py"
        _write_rule_file(rule_file, "x = 1\n")

        config = _config_with_custom(
            tmp_path,
            [_entry("custom/empty", "empty.py")],
        )
        with pytest.raises(ValueError, match="no @rule function found"):
            load_custom_rules(config)

    def test_id_mismatch(self, tmp_path):
        rule_file = tmp_path / "wrong_id.py"
        _write_rule_file(
            rule_file,
            """\
            from dbt_lint.extend import Resource, RuleConfig, Violation, rule

            @rule(id="custom/other-name", description="Wrong.")
            def other(resource: Resource, config: RuleConfig) -> Violation | None:
                return None
            """,
        )

        config = _config_with_custom(
            tmp_path,
            [_entry("custom/expected-name", "wrong_id.py")],
        )
        with pytest.raises(ValueError, match="no matching"):
            load_custom_rules(config)

    def test_builtin_id_collision(self, tmp_path):
        rule_file = tmp_path / "collision.py"
        _write_rule_file(
            rule_file,
            """\
            from dbt_lint.extend import Resource, RuleConfig, Violation, rule

            @rule(
                id="modeling/too-many-joins",
                description="Collides with built-in.",
            )
            def bad(resource: Resource, config: RuleConfig) -> Violation | None:
                return None
            """,
        )

        config = _config_with_custom(
            tmp_path,
            [_entry("modeling/too-many-joins", "collision.py")],
        )
        with pytest.raises(ValueError, match="conflicts with built-in"):
            load_custom_rules(config)

    def test_import_error_wrapped(self, tmp_path):
        rule_file = tmp_path / "bad_syntax.py"
        _write_rule_file(rule_file, "def broken(\n")

        config = _config_with_custom(
            tmp_path,
            [_entry("custom/bad", "bad_syntax.py")],
        )
        with pytest.raises(ImportError, match="Failed to load custom rule"):
            load_custom_rules(config)

    def test_no_config_dir_raises(self):
        config = Config(
            params={**DEFAULTS},
            include=None,
            exclude=None,
            config_dir=None,
            _rule_overrides={},
            _custom_rule_entries=[_entry("custom/x", "x.py")],
        )
        with pytest.raises(ValueError, match="require a config file"):
            load_custom_rules(config)


class TestSignatureValidation:
    def test_bad_signature_at_decoration_time(self, tmp_path):
        rule_file = tmp_path / "bad_sig.py"
        _write_rule_file(
            rule_file,
            """\
            from dbt_lint.extend import Resource, rule

            @rule(id="custom/bad-sig", description="Bad.")
            def bad(resource: Resource):
                return None
            """,
        )

        config = _config_with_custom(
            tmp_path,
            [_entry("custom/bad-sig", "bad_sig.py")],
        )
        with pytest.raises(ImportError, match="@rule error"):
            load_custom_rules(config)


class TestEmptyEntries:
    def test_no_custom_entries(self, tmp_path):
        config = _config_with_custom(tmp_path, [])
        assert not load_custom_rules(config)


class TestEndToEndWithConfig:
    def test_config_loading_separates_custom_entries(self, tmp_path):
        config_file = tmp_path / "dbt_lint.yml"
        rule_file = tmp_path / "custom_rules" / "my_rule.py"
        _write_rule_file(
            rule_file,
            """\
            from dbt_lint.extend import Resource, RuleConfig, Violation, rule

            @rule(id="custom/my-rule", description="My rule.")
            def my_rule(resource: Resource, config: RuleConfig) -> Violation | None:
                return None
            """,
        )

        config_file.write_text(
            textwrap.dedent("""\
            rules:
              modeling/too-many-joins:
                severity: error
              custom/my-rule:
                source: custom_rules/my_rule.py
                severity: warn
            """)
        )

        config = load_config(config_file)

        # Built-in override present
        rc = config.rule_config("modeling/too-many-joins")
        assert rc.severity == "error"

        # Custom entry parsed
        assert len(config._custom_rule_entries) == 1
        assert config._custom_rule_entries[0].rule_id == "custom/my-rule"
        assert config._custom_rule_entries[0].source == "custom_rules/my_rule.py"

        # Custom rule config accessible
        rc_custom = config.rule_config("custom/my-rule")
        assert rc_custom.severity == "warn"

        # Loader can resolve it
        rules = load_custom_rules(config)
        assert len(rules) == 1
        assert rules[0].id == "custom/my-rule"
