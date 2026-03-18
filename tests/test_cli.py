"""Tests for CLI: argument parsing, exit codes, format selection."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from dbt_linter.__main__ import main


def _write_manifest(tmp_path: Path) -> Path:
    """Write a minimal valid manifest that triggers known violations."""
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v11.json",
        },
        "nodes": {
            "model.pkg.stg_users": {
                "unique_id": "model.pkg.stg_users",
                "name": "stg_users",
                "resource_type": "model",
                "original_file_path": "models/staging/stg_users.sql",
                "raw_code": "select * from {{ ref('raw_users') }}",
                "description": "Staged users.",
                "schema": "public",
                "database": "analytics",
                "access": "protected",
                "contract": {"enforced": False},
                "config": {
                    "materialized": "view",
                    "tags": [],
                    "meta": {},
                },
                "columns": {
                    "user_id": {"name": "user_id", "description": "PK"},
                },
            },
            "model.pkg.fct_orders": {
                "unique_id": "model.pkg.fct_orders",
                "name": "fct_orders",
                "resource_type": "model",
                "original_file_path": "models/marts/fct_orders.sql",
                "raw_code": "select * from {{ ref('stg_users') }}",
                "description": "",
                "schema": "public",
                "database": "analytics",
                "access": "public",
                "contract": {"enforced": False},
                "config": {
                    "materialized": "table",
                    "tags": [],
                    "meta": {},
                },
                "columns": {
                    "order_id": {"name": "order_id", "description": ""},
                },
            },
            "test.pkg.unique_stg_users_user_id": {
                "unique_id": "test.pkg.unique_stg_users_user_id",
                "name": "unique_stg_users_user_id",
                "resource_type": "test",
                "test_metadata": {"name": "unique", "namespace": "dbt"},
                "attached_node": "model.pkg.stg_users",
            },
            "test.pkg.not_null_stg_users_user_id": {
                "unique_id": "test.pkg.not_null_stg_users_user_id",
                "name": "not_null_stg_users_user_id",
                "resource_type": "test",
                "test_metadata": {"name": "not_null", "namespace": "dbt"},
                "attached_node": "model.pkg.stg_users",
            },
        },
        "sources": {
            "source.pkg.raw.users": {
                "unique_id": "source.pkg.raw.users",
                "name": "users",
                "resource_type": "source",
                "original_file_path": "models/staging/_sources.yml",
                "source_description": "Raw user data.",
                "description": "Users table.",
                "schema": "raw",
                "database": "analytics",
                "meta": {},
                "freshness": {
                    "warn_after": {"count": 24, "period": "hour"},
                    "error_after": None,
                },
            },
        },
        "exposures": {},
        "parent_map": {
            "model.pkg.stg_users": ["source.pkg.raw.users"],
            "model.pkg.fct_orders": ["model.pkg.stg_users"],
        },
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return path


class TestCliBasic:
    """Basic CLI invocation and argument parsing."""

    def test_help_flag(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "manifest" in result.output.lower()

    def test_missing_manifest_exits_2(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(main, [str(tmp_path / "nonexistent.json")])
        assert result.exit_code == 2

    def test_invalid_manifest_exits_2(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json")
        runner = CliRunner()
        result = runner.invoke(main, [str(bad)])
        assert result.exit_code == 2


class TestCliFormats:
    """Output format selection."""

    def test_text_format_default(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path)])
        # Text output has category headers
        assert result.exit_code == 1  # violations found
        out = result.output.lower()
        assert "documentation" in out or "governance" in out

    def test_json_format(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path), "--format", "json"])
        assert result.exit_code == 1
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)
        assert len(parsed) > 0

    def test_text_format_explicit(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path), "--format", "text"])
        assert result.exit_code == 1
        assert "Found" in result.output


class TestCliExitCodes:
    """Exit code behavior with --fail-on."""

    def test_violations_exit_1(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path)])
        assert result.exit_code == 1

    def test_fail_on_error_exits_0_for_warnings(self, tmp_path):
        """With --fail-on error, warnings-only should exit 0."""
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path), "--fail-on", "error"])
        # Default severity is warn, so all violations are warnings
        assert result.exit_code == 0

    def test_fail_on_warn_exits_1(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path), "--fail-on", "warn"])
        assert result.exit_code == 1


class TestCliGitHubAnnotations:
    """GitHub Actions annotation output via GITHUB_ACTIONS env var."""

    def test_annotations_when_github_actions_env(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner(env={"GITHUB_ACTIONS": "true"})
        result = runner.invoke(main, [str(manifest_path), "--format", "text"])
        assert "::" in result.output

    def test_no_annotations_without_env(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner(env={})
        result = runner.invoke(main, [str(manifest_path), "--format", "text"])
        assert "::warning" not in result.output
        assert "::error" not in result.output


class TestCliConfig:
    """Config file loading."""

    def test_custom_config_disables_rule(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        config_path = tmp_path / "custom.yml"
        config_yaml = (
            "rules:\n  documentation/undocumented-models:\n    enabled: false\n"
        )
        config_path.write_text(config_yaml)
        runner = CliRunner()
        result = runner.invoke(
            main, [str(manifest_path), "--config", str(config_path), "--format", "json"]
        )
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        assert "documentation/undocumented-models" not in rule_ids


class TestCliFailFast:
    """--fail-fast flag for early exit."""

    def test_fail_fast_flag_accepted(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path), "--fail-fast"])
        assert result.exit_code == 1
        assert "Found" in result.output

    def test_fail_fast_fewer_violations(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        normal = runner.invoke(main, [str(manifest_path), "--format", "json"])
        fast = runner.invoke(
            main, [str(manifest_path), "--format", "json", "--fail-fast"]
        )
        normal_count = len(json.loads(normal.output))
        fast_count = len(json.loads(fast.output))
        assert fast_count <= normal_count
        assert fast_count >= 1


class TestCliSelectExclude:
    """--select and --exclude filter rules."""

    def test_select_single_rule(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                str(manifest_path),
                "--format",
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
                str(manifest_path),
                "--format",
                "json",
                "--exclude",
                "documentation/undocumented-models",
            ],
        )
        assert result.exit_code == 1
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        assert "documentation/undocumented-models" not in rule_ids


class TestCliBaselineLoading:
    """Loading and merging dbt-lint-baseline.yml."""

    def test_auto_discover_next_to_config(self, tmp_path):
        """Baseline file next to --config is auto-discovered and merged."""
        manifest_path = _write_manifest(tmp_path)
        config_path = tmp_path / "dbt-lint.yml"
        config_path.write_text("rules: {}\n")
        baseline_path = tmp_path / "dbt-lint-baseline.yml"
        baseline_path.write_text(
            "rules:\n  documentation/undocumented-models:\n    enabled: false\n"
        )
        runner = CliRunner()
        result = runner.invoke(
            main,
            [str(manifest_path), "--config", str(config_path), "--format", "json"],
        )
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        assert "documentation/undocumented-models" not in rule_ids

    def test_no_baseline_no_error(self, tmp_path):
        """Missing baseline file is silently ignored."""
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path)])
        assert result.exit_code in (0, 1)

    def test_explicit_baseline_flag(self, tmp_path):
        """--baseline with explicit path loads the file."""
        manifest_path = _write_manifest(tmp_path)
        baseline_path = tmp_path / "custom-baseline.yml"
        baseline_path.write_text(
            "rules:\n  documentation/undocumented-models:\n    enabled: false\n"
        )
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                str(manifest_path),
                "--baseline",
                str(baseline_path),
                "--format",
                "json",
            ],
        )
        parsed = json.loads(result.output)
        rule_ids = {v["rule_id"] for v in parsed}
        assert "documentation/undocumented-models" not in rule_ids

    def test_generate_baseline_skips_existing_baseline(self, tmp_path):
        """--generate-baseline ignores existing baseline to show full violations."""
        manifest_path = _write_manifest(tmp_path)
        config_path = tmp_path / "dbt-lint.yml"
        config_path.write_text("rules: {}\n")
        # Baseline that disables everything
        baseline_path = tmp_path / "dbt-lint-baseline.yml"
        baseline_path.write_text(
            "rules:\n  documentation/undocumented-models:\n    enabled: false\n"
        )
        runner = CliRunner()
        result = runner.invoke(
            main,
            [str(manifest_path), "--config", str(config_path), "--generate-baseline"],
        )
        assert result.exit_code == 0
        parsed = yaml.safe_load(result.output)
        # Should still contain undocumented-models since baseline was skipped
        assert "documentation/undocumented-models" in parsed["rules"]

    def test_round_trip_generate_then_load(self, tmp_path):
        """Generate baseline, then load it. Suppressed rules should vanish."""
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        # Step 1: generate baseline
        gen_result = runner.invoke(main, [str(manifest_path), "--generate-baseline"])
        assert gen_result.exit_code == 0
        baseline_path = tmp_path / "dbt-lint-baseline.yml"
        baseline_path.write_text(gen_result.output)
        # Step 2: run with baseline
        result = runner.invoke(
            main,
            [str(manifest_path), "--baseline", str(baseline_path), "--format", "json"],
        )
        parsed = json.loads(result.output)
        assert len(parsed) == 0


class TestCliListRules:
    """--list-rules flag for rule discovery."""

    def test_text_output_includes_totals(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--list-rules"])
        assert result.exit_code == 0
        assert "41 rules" in result.output

    def test_text_output_includes_categories(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--list-rules"])
        assert result.exit_code == 0
        assert "modeling" in result.output.lower()
        assert "documentation" in result.output.lower()

    def test_json_output_parses_to_41_items(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--list-rules", "--format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert len(parsed) == 41

    def test_no_manifest_required(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--list-rules"])
        assert result.exit_code == 0

    def test_manifest_still_required_without_flag(self):
        runner = CliRunner()
        result = runner.invoke(main, [])
        assert result.exit_code == 2


class TestCliGenerateBaseline:
    """--generate-baseline flag for producing suppressions config."""

    def test_outputs_valid_yaml(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path), "--generate-baseline"])
        assert result.exit_code == 0
        parsed = yaml.safe_load(result.output)
        assert "rules" in parsed

    def test_exits_0(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path), "--generate-baseline"])
        assert result.exit_code == 0

    def test_skips_normal_report(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path), "--generate-baseline"])
        assert "Found" not in result.output

    def test_contains_header_comment(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path), "--generate-baseline"])
        assert "Generated by dbt-lint" in result.output

    def test_contains_violated_rules(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(main, [str(manifest_path), "--generate-baseline"])
        parsed = yaml.safe_load(result.output)
        assert len(parsed["rules"]) > 0

    def test_output_file(self, tmp_path):
        manifest_path = _write_manifest(tmp_path)
        output_path = tmp_path / "baseline.yml"
        runner = CliRunner()
        result = runner.invoke(
            main,
            [str(manifest_path), "--generate-baseline", "--output", str(output_path)],
        )
        assert result.exit_code == 0
        assert output_path.exists()
        parsed = yaml.safe_load(output_path.read_text())
        assert "rules" in parsed

    def test_select_filters_config(self, tmp_path):
        """--select should limit which rules appear in the config."""
        manifest_path = _write_manifest(tmp_path)
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                str(manifest_path),
                "--generate-baseline",
                "--select",
                "documentation/undocumented-models",
            ],
        )
        assert result.exit_code == 0
        parsed = yaml.safe_load(result.output)
        assert set(parsed["rules"].keys()) <= {"documentation/undocumented-models"}
