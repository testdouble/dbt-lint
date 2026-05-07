"""Tests for reporter module: text, JSON, and GitHub Actions annotation output."""

from __future__ import annotations

import json

import pytest

from dbt_lint.models import Violation
from dbt_lint.reporter import _format_annotations, report


class TestTextReport:
    """Text output: grouped by category, then rule."""

    def test_empty_violations_returns_clean_message(self):
        result = report([], output_format="text")
        assert result == "Found 0 violations"

    def test_single_violation(self, make_violation):
        violations = [make_violation()]
        result = report(violations, output_format="text")
        assert "documentation" in result.lower()
        assert "undocumented-models" in result
        assert "stg_users" in result

    def test_groups_by_category(self, make_violation):
        violations = [
            make_violation(
                rule_id="documentation/undocumented-models", resource_name="m1"
            ),
            make_violation(
                rule_id="governance/public-models-without-contract",
                resource_name="m2",
                message="m2: public without contract",
            ),
        ]
        result = report(violations, output_format="text")
        # Both categories should appear as section headers
        assert "documentation" in result.lower()
        assert "governance" in result.lower()

    def test_groups_by_rule_within_category(self, make_violation):
        violations = [
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_name="m1",
                message="m1: missing description",
            ),
            make_violation(
                rule_id="documentation/documentation-coverage",
                resource_name="m2",
                message="m2: below coverage target",
                resource_id="model.pkg.m2",
            ),
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_name="m3",
                message="m3: missing description",
                resource_id="model.pkg.m3",
            ),
        ]
        result = report(violations, output_format="text")
        lines = result.split("\n")
        # Both rule IDs from documentation category should appear
        assert any("undocumented-models" in line for line in lines)
        assert any("documentation-coverage" in line for line in lines)

    def test_file_path_shown_on_violation(self, make_violation):
        violations = [make_violation(file_path="models/staging/stg_users.sql")]
        result = report(violations, output_format="text")
        assert "--> models/staging/stg_users.sql" in result

    def test_patch_path_shown_when_present(self, make_violation):
        violations = [
            make_violation(patch_path="models/staging/_staging.yml"),
        ]
        result = report(violations, output_format="text")
        assert "yml: models/staging/_staging.yml" in result

    def test_patch_path_absent_when_empty(self, make_violation):
        violations = [make_violation(patch_path="")]
        result = report(violations, output_format="text")
        assert "yml:" not in result

    def test_file_path_arrow_absent_when_empty(self, make_violation):
        violations = [make_violation(file_path="")]
        result = report(violations, output_format="text")
        assert "-->" not in result

    def test_severity_tag_on_errors(self, make_violation):
        violations = [make_violation(severity="error")]
        result = report(violations, output_format="text")
        assert "[error]" in result

    def test_severity_tag_on_warnings(self, make_violation):
        violations = [make_violation(severity="warn")]
        result = report(violations, output_format="text")
        assert "[warn]" in result

    def test_summary_line_with_counts(self, make_violation):
        violations = [
            make_violation(severity="warn"),
            make_violation(
                severity="error",
                resource_name="m2",
                resource_id="model.pkg.m2",
                message="m2: missing description",
            ),
        ]
        result = report(violations, output_format="text")
        assert "Found 2 violations" in result

    def test_multiple_violations_same_rule(self, make_violation):
        violations = [
            make_violation(resource_name="m1", message="m1: missing description"),
            make_violation(
                resource_name="m2",
                resource_id="model.pkg.m2",
                message="m2: missing description",
            ),
        ]
        result = report(violations, output_format="text")
        assert "m1" in result
        assert "m2" in result


class TestConciseReport:
    """Concise output: one line per violation, summary at end."""

    def test_empty_violations(self):
        result = report([], output_format="concise")
        assert result == "Found 0 violations"

    def test_single_violation_one_line(self, make_violation):
        violations = [
            make_violation(
                file_path="models/staging/stg_users.sql",
                severity="error",
                rule_id="documentation/undocumented-models",
                message="stg_users: missing description",
            ),
        ]
        result = report(violations, output_format="concise")
        lines = [line for line in result.strip().split("\n") if line]
        # First line: file_path: [severity] category/rule: message
        assert lines[0] == (
            "models/staging/stg_users.sql: [error] documentation/undocumented-models:"
            " stg_users: missing description"
        )

    def test_multiple_violations(self, make_violation):
        violations = [
            make_violation(
                file_path="models/staging/stg_users.sql",
                severity="warn",
                message="stg_users: missing description",
            ),
            make_violation(
                file_path="models/staging/stg_orders.sql",
                severity="error",
                resource_id="model.pkg.stg_orders",
                message="stg_orders: missing description",
            ),
        ]
        result = report(violations, output_format="concise")
        assert "stg_users: missing description" in result
        assert "stg_orders: missing description" in result
        assert "Found 2 violations" in result

    def test_summary_line(self, make_violation):
        violations = [
            make_violation(severity="warn"),
            make_violation(
                severity="error",
                resource_id="model.pkg.m2",
                message="m2: issue",
            ),
        ]
        result = report(violations, output_format="concise")
        assert "Found 2 violations" in result

    def test_empty_file_path(self, make_violation):
        violations = [make_violation(file_path="")]
        result = report(violations, output_format="concise")
        lines = [line for line in result.strip().split("\n") if line]
        assert lines[0].startswith("(no file):")


class TestGroupedReport:
    """Grouped output: violations grouped by file path."""

    def test_empty_violations(self):
        result = report([], output_format="grouped")
        assert result == "Found 0 violations"

    def test_single_file(self, make_violation):
        violations = [
            make_violation(
                file_path="models/staging/stg_users.sql",
                severity="error",
                rule_id="documentation/undocumented-models",
                message="stg_users: missing description",
            ),
        ]
        result = report(violations, output_format="grouped")
        assert "models/staging/stg_users.sql" in result
        assert (
            "[error] documentation/undocumented-models: stg_users: missing description"
            in result
        )

    def test_multiple_files(self, make_violation):
        violations = [
            make_violation(
                file_path="models/staging/stg_users.sql",
                message="stg_users: issue",
            ),
            make_violation(
                file_path="models/staging/stg_orders.sql",
                resource_id="model.pkg.stg_orders",
                message="stg_orders: issue",
            ),
        ]
        result = report(violations, output_format="grouped")
        assert "models/staging/stg_users.sql" in result
        assert "models/staging/stg_orders.sql" in result

    def test_multiple_violations_same_file(self, make_violation):
        violations = [
            make_violation(
                file_path="models/staging/stg_users.sql",
                rule_id="documentation/undocumented-models",
                message="stg_users: missing description",
            ),
            make_violation(
                file_path="models/staging/stg_users.sql",
                rule_id="documentation/undocumented-columns",
                message="stg_users: 3 columns undocumented",
                resource_id="model.pkg.stg_users_2",
            ),
        ]
        result = report(violations, output_format="grouped")
        lines = result.strip().split("\n")
        # File header should appear once
        file_lines = [line for line in lines if line == "models/staging/stg_users.sql"]
        assert len(file_lines) == 1
        # Both violations indented under it
        assert any("undocumented-models" in line for line in lines)
        assert any("undocumented-columns" in line for line in lines)

    def test_empty_file_path_grouped_as_no_file(self, make_violation):
        violations = [make_violation(file_path="")]
        result = report(violations, output_format="grouped")
        assert "(no file)" in result

    def test_summary(self, make_violation):
        violations = [
            make_violation(severity="warn"),
            make_violation(
                severity="error",
                resource_id="model.pkg.m2",
                message="m2: issue",
            ),
        ]
        result = report(violations, output_format="grouped")
        assert "Found 2 violations" in result


class TestJsonReport:
    """JSON output: list of violation objects."""

    def test_empty_violations_returns_empty_list(self):
        result = report([], output_format="json")
        assert not json.loads(result)

    def test_single_violation_structure(self, make_violation):
        violations = [
            make_violation(
                rule_id="documentation/undocumented-models",
                resource_id="model.pkg.stg_users",
                resource_name="stg_users",
                message="stg_users: missing description",
                severity="warn",
                file_path="models/staging/stg_users.sql",
                patch_path="models/staging/_staging.yml",
            )
        ]
        result = json.loads(report(violations, output_format="json"))
        assert len(result) == 1
        obj = result[0]
        assert obj["rule_id"] == "documentation/undocumented-models"
        assert obj["resource_id"] == "model.pkg.stg_users"
        assert obj["resource_name"] == "stg_users"
        assert obj["message"] == "stg_users: missing description"
        assert obj["severity"] == "warn"
        assert obj["file_path"] == "models/staging/stg_users.sql"
        assert obj["patch_path"] == "models/staging/_staging.yml"

    def test_multiple_violations(self, make_violation):
        violations = [
            make_violation(resource_name="m1"),
            make_violation(resource_name="m2", resource_id="model.pkg.m2"),
        ]
        result = json.loads(report(violations, output_format="json"))
        assert len(result) == 2
        names = {obj["resource_name"] for obj in result}
        assert names == {"m1", "m2"}

    def test_json_roundtrips_special_characters(self, make_violation):
        violations = [make_violation(message='has "quotes" and\nnewlines')]
        result = report(violations, output_format="json")
        parsed = json.loads(result)
        assert len(parsed) == 1
        assert parsed[0]["message"] == 'has "quotes" and\nnewlines'


class TestGitHubAnnotations:
    """GitHub Actions ::error/::warning workflow commands."""

    def test_warning_annotation_format(self, make_violation):
        violations = [make_violation(severity="warn")]
        result = report(violations, output_format="text", github_annotations=True)
        assert "::warning file=models/staging/stg_users.sql" in result
        assert "title=documentation/undocumented-models" in result
        assert "::stg_users%3A missing description" in result

    def test_error_annotation_format(self, make_violation):
        violations = [make_violation(severity="error")]
        result = report(violations, output_format="text", github_annotations=True)
        assert "::error file=models/staging/stg_users.sql" in result

    def test_multiple_annotations(self, make_violation):
        violations = [
            make_violation(severity="warn", resource_name="m1", message="m1: issue"),
            make_violation(
                severity="error",
                resource_name="m2",
                resource_id="model.pkg.m2",
                message="m2: issue",
            ),
        ]
        result = report(violations, output_format="text", github_annotations=True)
        assert result.count("::warning") == 1
        assert result.count("::error") == 1

    def test_annotations_precede_text_output(self, make_violation):
        violations = [make_violation()]
        result = report(violations, output_format="text", github_annotations=True)
        lines = result.split("\n")
        annotation_idx = next(
            i for i, line in enumerate(lines) if line.startswith("::")
        )
        # Text header comes after annotations
        text_idx = next(
            i
            for i, line in enumerate(lines)
            if "documentation" in line.lower() and not line.startswith("::")
        )
        assert annotation_idx < text_idx

    def test_no_annotations_without_flag(self, make_violation):
        violations = [make_violation()]
        result = report(violations, output_format="text", github_annotations=False)
        assert "::" not in result

    def test_empty_violations_no_annotations(self):
        result = report([], output_format="text", github_annotations=True)
        assert "::" not in result

    @pytest.mark.parametrize(
        ("field", "value", "expected"),
        [
            ("message", "line1\nline2", "%0A"),
            ("message", "has\rreturn", "%0D"),
            ("message", "100% complete", "%25"),
            ("file_path", "models/a,b.sql", "a%2Cb.sql"),
            ("rule_id", "custom:rule", "custom%3Arule"),
        ],
    )
    def test_special_characters_escaped(self, make_violation, field, value, expected):
        violations = [make_violation(**{field: value})]
        result = _format_annotations(violations)
        assert "\n" not in result
        assert expected in result

    def test_annotation_injection_blocked(self):
        violation = Violation(
            rule_id="r/x",
            resource_id="model.pkg.x",
            resource_name="x",
            message="msg\n::error file=injected::payload",
            severity="warn",
            file_path="f.sql",
        )
        result = _format_annotations([violation])
        assert result.count("\n") == 0
        assert "%0A" in result


class TestColorSupport:
    """Color output via click.style()."""

    def test_error_severity_colored_red(self, make_violation):
        violations = [make_violation(severity="error")]
        result = report(violations, output_format="text", color=True)
        # click.style wraps with ANSI codes; red is \x1b[31m
        assert "\x1b[" in result
        assert "[error]" in result

    def test_warn_severity_colored_yellow(self, make_violation):
        violations = [make_violation(severity="warn")]
        result = report(violations, output_format="text", color=True)
        assert "\x1b[" in result
        assert "[warn]" in result

    def test_no_color_when_disabled(self, make_violation):
        violations = [make_violation(severity="error")]
        result = report(violations, output_format="text", color=False)
        assert "\x1b[" not in result

    def test_no_color_in_json(self, make_violation):
        violations = [make_violation()]
        result = report(violations, output_format="json", color=True)
        assert "\x1b[" not in result

    def test_color_in_concise(self, make_violation):
        violations = [make_violation(severity="error")]
        result = report(violations, output_format="concise", color=True)
        assert "\x1b[" in result

    def test_color_in_grouped(self, make_violation):
        violations = [make_violation(severity="error")]
        result = report(violations, output_format="grouped", color=True)
        assert "\x1b[" in result

    def test_grouped_file_header_bold(self, make_violation):
        violations = [make_violation(file_path="models/stg_users.sql")]
        result = report(violations, output_format="grouped", color=True)
        # Bold is \x1b[1m
        assert "\x1b[1m" in result


class TestUnifiedSummary:
    """Unified 'Inspected X. Found Y' summary line, shared across formats."""

    def test_zero_violations_zero_excluded(self):
        result = report([], output_format="text")
        assert result == "Found 0 violations"

    def test_zero_violations_with_excluded(self):
        result = report([], output_format="text", excluded=42)
        assert result == "Found 0 violations (42 skipped)"

    def test_single_severity_warn_only(self, make_violation):
        violations = [
            make_violation(severity="warn"),
            make_violation(
                severity="warn", resource_id="model.pkg.m2", resource_name="m2"
            ),
        ]
        result = report(violations, output_format="text")
        assert "Found 2 warnings" in result
        assert "violations" not in result.split("\n")[-1]

    def test_single_severity_error_only(self, make_violation):
        violations = [make_violation(severity="error")]
        result = report(violations, output_format="text")
        assert "Found 1 error" in result
        # Drop trailing-s: "1 error" not "1 errors"
        assert "1 errors" not in result

    def test_mixed_severity_uses_violations_with_breakdown(self, make_violation):
        violations = [
            make_violation(severity="error"),
            make_violation(
                severity="warn", resource_id="model.pkg.m2", resource_name="m2"
            ),
            make_violation(
                severity="warn", resource_id="model.pkg.m3", resource_name="m3"
            ),
        ]
        result = report(violations, output_format="text")
        assert "Found 3 violations (1 error, 2 warnings)" in result

    def test_single_severity_with_excluded_appends_skipped(self, make_violation):
        result = report([make_violation(severity="warn")], excluded=10)
        assert "Found 1 warning (10 skipped)" in result

    def test_mixed_severity_with_excluded_in_same_parenthetical(self, make_violation):
        violations = [
            make_violation(severity="error"),
            make_violation(
                severity="warn", resource_id="model.pkg.m2", resource_name="m2"
            ),
        ]
        result = report(violations, output_format="text", excluded=5)
        assert "Found 2 violations (1 error, 1 warning, 5 skipped)" in result

    def test_via_config_phrase_dropped(self, make_violation):
        result = report([make_violation()], excluded=10)
        assert "via config" not in result

    def test_concise_uses_same_summary(self, make_violation):
        result = report(
            [make_violation(severity="warn")], output_format="concise", excluded=5
        )
        assert "Found 1 warning (5 skipped)" in result

    def test_grouped_uses_same_summary(self, make_violation):
        result = report(
            [make_violation(severity="warn")], output_format="grouped", excluded=5
        )
        assert "Found 1 warning (5 skipped)" in result


class TestScopePrefix:
    """Optional 'Inspected N models, ...' prefix from resource_counts."""

    def test_prefix_added_when_resource_counts_present(self, make_violation):
        result = report(
            [make_violation(severity="warn")],
            output_format="text",
            resource_counts={"model": 1034, "source": 542},
        )
        assert "Inspected 1034 models, 542 sources. Found 1 warning" in result

    def test_singular_pluralization(self, make_violation):
        result = report(
            [make_violation(severity="error")],
            output_format="text",
            resource_counts={"model": 1, "source": 1},
        )
        assert "Inspected 1 model, 1 source. Found 1 error" in result

    def test_zero_counts_dropped_from_scope(self, make_violation):
        result = report(
            [make_violation(severity="warn")],
            output_format="text",
            resource_counts={"model": 5, "exposure": 0, "source": 2},
        )
        assert "Inspected 5 models, 2 sources. Found 1 warning" in result
        assert "exposure" not in result.split("\n")[-1]

    def test_scope_alphabetical_order(self, make_violation):
        result = report(
            [make_violation(severity="warn")],
            output_format="text",
            resource_counts={"source": 1, "model": 2, "exposure": 3},
        )
        assert "Inspected 3 exposures, 2 models, 1 source." in result

    def test_no_prefix_when_resource_counts_omitted(self, make_violation):
        result = report([make_violation(severity="warn")], output_format="text")
        assert "Inspected" not in result

    def test_no_prefix_when_resource_counts_empty(self, make_violation):
        result = report(
            [make_violation(severity="warn")],
            output_format="text",
            resource_counts={},
        )
        assert "Inspected" not in result

    def test_prefix_with_zero_violations(self):
        result = report(
            [], output_format="text", resource_counts={"model": 1034, "source": 542}
        )
        assert result == "Inspected 1034 models, 542 sources. Found 0 violations"

    def test_prefix_with_zero_violations_and_excluded(self):
        result = report(
            [],
            output_format="text",
            excluded=553,
            resource_counts={"model": 1034, "source": 542},
        )
        assert (
            result
            == "Inspected 1034 models, 542 sources. Found 0 violations (553 skipped)"
        )

    def test_concise_format_gets_prefix(self, make_violation):
        result = report(
            [make_violation(severity="warn")],
            output_format="concise",
            resource_counts={"model": 4},
        )
        assert "Inspected 4 models. Found 1 warning" in result

    def test_grouped_format_gets_prefix(self, make_violation):
        result = report(
            [make_violation(severity="warn")],
            output_format="grouped",
            resource_counts={"model": 4},
        )
        assert "Inspected 4 models. Found 1 warning" in result

    def test_json_format_omits_prefix(self, make_violation):
        result = report(
            [make_violation()],
            output_format="json",
            resource_counts={"model": 4},
        )
        assert "Inspected" not in result
        # JSON shape unchanged
        parsed = json.loads(result)
        assert isinstance(parsed, list)
