"""Tests for CLI: subcommand dispatch, argument parsing, exit codes, format selection."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dbt_lint.__main__ import main
from dbt_lint.rules import get_all_rules
from helpers import fixture_manifest_dict

EXIT_USAGE = 2


@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    """Run every test from a clean tmp_path so config walk-up can't reach real files."""
    monkeypatch.chdir(tmp_path)


def _write_manifest(tmp_path: Path) -> Path:
    """Write the shared fixture manifest to disk."""
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(fixture_manifest_dict()))
    return path


class TestTopLevelGroup:
    """Bare `dbt-lint` and unknown commands."""

    def test_bare_invocation_prints_help(self):
        runner = CliRunner()
        result = runner.invoke(main, [])
        # No-args on a click group returns 0 by default (no_args_is_help=True).
        assert result.exit_code in (0, 2)
        assert "check" in result.output
        assert "rule" in result.output

    def test_help_lists_examples_before_commands(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert result.output.index("Examples:") < result.output.index("Commands:")
        assert "dbt-lint check" in result.output
        assert "dbt-lint rule --all" in result.output

    def test_unknown_command_errors(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path)])
        assert result.exit_code == EXIT_USAGE


class TestCheckBasic:
    """`dbt-lint check` argument parsing and dispatch."""

    def test_no_args_prints_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["check"])
        assert result.exit_code == EXIT_USAGE
        assert "MANIFEST" in result.output

    def test_help_flag(self):
        runner = CliRunner()
        result = runner.invoke(main, ["check", "--help"])
        assert result.exit_code == 0
        assert "manifest" in result.output.lower()

    def test_missing_manifest_exits_2(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(main, ["check", str(tmp_path / "nonexistent.json")])
        assert result.exit_code == EXIT_USAGE

    def test_invalid_manifest_exits_2(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json")
        runner = CliRunner()
        result = runner.invoke(main, ["check", str(bad)])
        assert result.exit_code == EXIT_USAGE


class TestCheckFormats:
    """Output format selection on check."""

    def test_text_format_default(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, ["check", str(manifest_path)])
        assert result.exit_code == 1
        out = result.output.lower()
        assert "documentation" in out or "governance" in out

    def test_json_format(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main, ["check", str(manifest_path), "--output-format", "json"]
        )
        assert result.exit_code == 1
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)
        assert parsed

    def test_text_format_explicit(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main, ["check", str(manifest_path), "--output-format", "text"]
        )
        assert result.exit_code == 1
        assert "Found" in result.output


class TestCheckExitCodes:
    """Exit code behavior on check."""

    def test_violations_exit_1(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, ["check", str(manifest_path)])
        assert result.exit_code == 1

    def test_fail_on_error_exits_0_for_warnings(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main, ["check", str(manifest_path), "--fail-on", "error"]
        )
        assert result.exit_code == 0

    def test_fail_on_warn_exits_1(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, ["check", str(manifest_path), "--fail-on", "warn"])
        assert result.exit_code == 1

    def test_exit_zero_overrides_violations(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, ["check", str(manifest_path), "--exit-zero"])
        assert result.exit_code == 0
        assert "Found" in result.output

    def test_exit_zero_composes_with_write_suppressions(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["check", str(manifest_path), "--exit-zero", "--write-suppressions"],
        )
        assert result.exit_code == 0


class TestCheckGitHubAnnotations:
    """GitHub Actions annotation output via GITHUB_ACTIONS env var."""

    def test_annotations_when_github_actions_env(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner(env={"GITHUB_ACTIONS": "true"})
        result = runner.invoke(
            main, ["check", str(manifest_path), "--output-format", "text"]
        )
        assert "::" in result.output

    def test_no_annotations_without_env(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner(env={"GITHUB_ACTIONS": None})
        result = runner.invoke(
            main, ["check", str(manifest_path), "--output-format", "text"]
        )
        assert "::warning" not in result.output
        assert "::error" not in result.output


class TestCheckConfig:
    """Config file loading on check."""

    def test_custom_config_disables_rule(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        config_path = tmp_path / "custom.yml"
        config_yaml = (
            "rules:\n  documentation/undocumented-models:\n    enabled: false\n"
        )
        config_path.write_text(config_yaml)
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "check",
                str(manifest_path),
                "--config",
                str(config_path),
                "--output-format",
                "json",
            ],
        )
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        assert "documentation/undocumented-models" not in rule_ids


class TestCheckIsolated:
    """--isolated bypasses both config discovery and suppressions auto-load."""

    def test_isolated_skips_config_discovery(self, tmp_path, monkeypatch):
        manifest_path = _write_manifest(tmp_path)
        config_path = tmp_path / "dbt-lint.yml"
        config_path.write_text(
            "rules:\n  documentation/undocumented-models:\n    enabled: false\n"
        )
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["check", str(manifest_path), "--isolated", "--output-format", "json"],
        )
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        # Without isolated, this rule would be disabled by the discovered config.
        assert "documentation/undocumented-models" in rule_ids

    def test_isolated_skips_suppressions_auto_load(self, tmp_path, monkeypatch):
        manifest_path = _write_manifest(tmp_path)
        suppressions_path = tmp_path / ".dbt-lint-suppressions.yml"
        suppressions_path.write_text(
            "rules:\n  documentation/undocumented-models:\n    enabled: false\n"
        )
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["check", str(manifest_path), "--isolated", "--output-format", "json"],
        )
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        # Without isolated, auto-load would disable this rule.
        assert "documentation/undocumented-models" in rule_ids


class TestCheckFailFast:
    def test_fail_fast_flag_accepted(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, ["check", str(manifest_path), "--fail-fast"])
        assert result.exit_code == 1
        assert "Found" in result.output


class TestCheckSelectExclude:
    def test_select_single_rule(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "check",
                str(manifest_path),
                "--output-format",
                "json",
                "--select",
                "documentation/undocumented-models",
            ],
        )
        assert result.exit_code == 1
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        assert rule_ids == {"documentation/undocumented-models"}

    def test_exclude_single_rule(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "check",
                str(manifest_path),
                "--output-format",
                "json",
                "--exclude",
                "documentation/undocumented-models",
            ],
        )
        assert result.exit_code == 1
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        assert "documentation/undocumented-models" not in rule_ids


class TestCheckSuppressions:
    """Loading and merging .dbt-lint-suppressions.yml under check."""

    def test_auto_discover_next_to_config(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        config_path = tmp_path / "dbt-lint.yml"
        config_path.write_text("rules: {}\n")
        suppressions_path = tmp_path / ".dbt-lint-suppressions.yml"
        suppressions_path.write_text(
            "rules:\n  documentation/undocumented-models:\n    enabled: false\n"
        )
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "check",
                str(manifest_path),
                "--config",
                str(config_path),
                "--output-format",
                "json",
            ],
        )
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        assert "documentation/undocumented-models" not in rule_ids

    def test_auto_discover_in_cwd(self, tmp_path, monkeypatch):
        manifest_path = _write_manifest(tmp_path)
        suppressions_path = tmp_path / ".dbt-lint-suppressions.yml"
        suppressions_path.write_text(
            "rules:\n  documentation/undocumented-models:\n    enabled: false\n"
        )
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main, ["check", str(manifest_path), "--output-format", "json"]
        )
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        assert "documentation/undocumented-models" not in rule_ids

    def test_no_suppressions_file_no_error(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, ["check", str(manifest_path)])
        assert result.exit_code in (0, 1)

    def test_explicit_suppressions_flag(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        suppressions_path = tmp_path / "custom-suppressions.yml"
        suppressions_path.write_text(
            "rules:\n  documentation/undocumented-models:\n    enabled: false\n"
        )
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "check",
                str(manifest_path),
                "--suppressions",
                str(suppressions_path),
                "--output-format",
                "json",
            ],
        )
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        assert "documentation/undocumented-models" not in rule_ids

    def test_write_suppressions_skips_auto_load(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        config_path = tmp_path / "dbt-lint.yml"
        config_path.write_text("rules: {}\n")
        suppressions_path = tmp_path / ".dbt-lint-suppressions.yml"
        suppressions_path.write_text(
            "rules:\n  documentation/undocumented-models:\n    enabled: false\n"
        )
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "check",
                str(manifest_path),
                "--config",
                str(config_path),
                "--write-suppressions",
            ],
        )
        assert result.exit_code == 0
        parsed = yaml.safe_load(result.output)
        assert "documentation/undocumented-models" in parsed["rules"]

    def test_round_trip_generate_then_load(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        gen_result = runner.invoke(
            main, ["check", str(manifest_path), "--write-suppressions"]
        )
        assert gen_result.exit_code == 0
        suppressions_path = tmp_path / ".dbt-lint-suppressions.yml"
        suppressions_path.write_text(gen_result.output)
        result = runner.invoke(
            main,
            [
                "check",
                str(manifest_path),
                "--suppressions",
                str(suppressions_path),
                "--output-format",
                "json",
            ],
        )
        parsed = json.loads(result.output)
        assert len(parsed) == 0


class TestCheckWriteSuppressions:
    """--write-suppressions emits YAML to stdout."""

    def test_outputs_valid_yaml(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main, ["check", str(manifest_path), "--write-suppressions"]
        )
        assert result.exit_code == 0
        parsed = yaml.safe_load(result.output)
        assert "rules" in parsed

    def test_exits_0(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main, ["check", str(manifest_path), "--write-suppressions"]
        )
        assert result.exit_code == 0

    def test_skips_normal_report(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main, ["check", str(manifest_path), "--write-suppressions"]
        )
        assert "Found" not in result.output

    def test_contains_header_comment(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main, ["check", str(manifest_path), "--write-suppressions"]
        )
        assert "Generated by dbt-lint" in result.output

    def test_contains_violated_rules(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main, ["check", str(manifest_path), "--write-suppressions"]
        )
        parsed = yaml.safe_load(result.output)
        assert parsed["rules"]

    def test_select_filters_config(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "check",
                str(manifest_path),
                "--write-suppressions",
                "--select",
                "documentation/undocumented-models",
            ],
        )
        assert result.exit_code == 0
        parsed = yaml.safe_load(result.output)
        assert set(parsed["rules"].keys()) <= {"documentation/undocumented-models"}


class TestRule:
    """`dbt-lint rule` listing and explain entry points."""

    def test_no_args_exits_non_zero_with_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["rule"])
        assert result.exit_code == EXIT_USAGE
        assert "Usage" in result.output

    def test_all_text_output_includes_totals(self):
        runner = CliRunner()
        result = runner.invoke(main, ["rule", "--all"])
        assert result.exit_code == 0
        assert f"{len(get_all_rules())} rules" in result.output

    def test_all_text_output_includes_categories(self):
        runner = CliRunner()
        result = runner.invoke(main, ["rule", "--all"])
        assert result.exit_code == 0
        assert "modeling" in result.output.lower()
        assert "documentation" in result.output.lower()

    def test_all_json_output_matches_rule_count(self):
        runner = CliRunner()
        result = runner.invoke(main, ["rule", "--all", "--output-format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert len(parsed) == len(get_all_rules())

    def test_explain_known_rule(self):
        rule_id = next(iter(get_all_rules())).id
        runner = CliRunner()
        result = runner.invoke(main, ["rule", rule_id])
        assert result.exit_code == 0
        assert rule_id in result.output

    def test_explain_unknown_rule_exits_2(self):
        runner = CliRunner()
        result = runner.invoke(main, ["rule", "no/such-rule"])
        assert result.exit_code == EXIT_USAGE
        assert "unknown rule" in result.output.lower()


def _write_custom_rule(tmp_path: Path) -> Path:
    """Write a minimal custom rule that flags models containing SELECT *."""
    rule_file = tmp_path / "no_select_star.py"
    rule_file.write_text(
        "from dbt_lint.extend import Resource, RuleContext, Violation, rule\n"
        "\n"
        '@rule(id="custom/no-select-star", description="Model uses SELECT *.")\n'
        "def no_select_star(resource: Resource, context: RuleContext)"
        " -> Violation | None:\n"
        '    if resource.raw_code and "select *" in resource.raw_code.lower():\n'
        "        return context.violation(resource, "
        '"uses SELECT *")\n'
        "    return None\n"
    )
    return rule_file


def _write_custom_config(tmp_path: Path, rule_file: Path, **overrides) -> Path:
    """Write a config with a custom rule source directive."""
    rule_cfg: dict = {"source": rule_file.name, **overrides}
    config = {"rules": {"custom/no-select-star": rule_cfg}}
    config_path = tmp_path / "dbt-lint.yml"
    config_path.write_text(yaml.dump(config))
    return config_path


class TestCheckCustomRule:
    """E2E: custom rule loaded via source directive produces CLI output."""

    def test_custom_rule_violation_in_json_output(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        rule_file = _write_custom_rule(tmp_path)
        config_path = _write_custom_config(tmp_path, rule_file)
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "check",
                str(manifest_path),
                "--config",
                str(config_path),
                "--output-format",
                "json",
            ],
        )
        assert result.exit_code == 1
        parsed = json.loads(result.output)
        custom_violations = [
            v for v in parsed if v["rule_id"] == "custom/no-select-star"
        ]
        assert custom_violations

    def test_custom_rule_disabled(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        rule_file = _write_custom_rule(tmp_path)
        config_path = _write_custom_config(tmp_path, rule_file, enabled=False)
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "check",
                str(manifest_path),
                "--config",
                str(config_path),
                "--output-format",
                "json",
            ],
        )
        parsed = json.loads(result.output)
        custom_violations = [
            v for v in parsed if v["rule_id"] == "custom/no-select-star"
        ]
        assert not custom_violations

    def test_custom_rule_severity_override(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        rule_file = _write_custom_rule(tmp_path)
        config_path = _write_custom_config(tmp_path, rule_file, severity="error")
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "check",
                str(manifest_path),
                "--config",
                str(config_path),
                "--output-format",
                "json",
            ],
        )
        assert result.exit_code == 1
        parsed = json.loads(result.output)
        custom_violations = [
            v for v in parsed if v["rule_id"] == "custom/no-select-star"
        ]
        assert custom_violations
        assert all(v["severity"] == "error" for v in custom_violations)
