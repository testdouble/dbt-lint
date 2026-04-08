"""Tests for reporter module: text, JSON, and GitHub Actions annotation output."""

from __future__ import annotations

import json

from dbt_lint.reporter import report


class TestTextReport:
    """Text output: grouped by category, then rule."""

    def test_empty_violations_returns_clean_message(self):
        result = report([], output_format="text")
        assert "no violations" in result.lower()

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

    def test_severity_shown_for_errors(self, make_violation):
        violations = [make_violation(severity="error")]
        result = report(violations, output_format="text")
        assert "error" in result.lower()

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

    def test_summary_includes_category_breakdown(self, make_violation):
        violations = [
            make_violation(
                rule_id="documentation/undocumented-models", resource_name="m1"
            ),
            make_violation(
                rule_id="governance/public-models-without-contract",
                resource_name="m2",
                message="m2: public without contract",
            ),
            make_violation(
                rule_id="documentation/documentation-coverage",
                resource_name="m3",
                message="m3: below coverage target",
                resource_id="model.pkg.m3",
            ),
        ]
        result = report(violations, output_format="text")
        assert "documentation (2)" in result
        assert "governance (1)" in result

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


class TestJsonReport:
    """JSON output: list of violation objects."""

    def test_empty_violations_returns_empty_list(self):
        result = report([], output_format="json")
        assert not json.loads(result)

    def test_single_violation_structure(self, make_violation):
        violations = [make_violation()]
        result = json.loads(report(violations, output_format="json"))
        assert len(result) == 1
        obj = result[0]
        assert obj["rule_id"] == "documentation/undocumented-models"
        assert obj["resource_id"] == "model.pkg.stg_users"
        assert obj["resource_name"] == "stg_users"
        assert obj["message"] == "stg_users: missing description"
        assert obj["severity"] == "warn"
        assert obj["file_path"] == "models/staging/stg_users.sql"

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
        assert "::stg_users: missing description" in result

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

    def test_newline_in_message_escaped(self, make_violation):
        violations = [make_violation(message="line1\nline2")]
        result = report(violations, output_format="text", github_annotations=True)
        annotation_lines = [
            line for line in result.split("\n") if line.startswith("::")
        ]
        assert len(annotation_lines) == 1
        assert "%0A" in annotation_lines[0]

    def test_carriage_return_in_message_escaped(self, make_violation):
        violations = [make_violation(message="has\rreturn")]
        result = report(violations, output_format="text", github_annotations=True)
        annotation_lines = [
            line for line in result.split("\n") if line.startswith("::")
        ]
        assert len(annotation_lines) == 1
        assert "%0D" in annotation_lines[0]

    def test_percent_in_message_escaped(self, make_violation):
        violations = [make_violation(message="100% complete")]
        result = report(violations, output_format="text", github_annotations=True)
        annotation_lines = [
            line for line in result.split("\n") if line.startswith("::")
        ]
        assert "%25" in annotation_lines[0]

    def test_annotation_injection_blocked(self, make_violation):
        violations = [make_violation(message="msg\n::error file=injected::payload")]
        result = report(violations, output_format="text", github_annotations=True)
        annotation_section = result.split("\n\n")[0]
        annotation_lines = [
            line for line in annotation_section.split("\n") if line.startswith("::")
        ]
        assert len(annotation_lines) == 1
        assert "%0A" in annotation_lines[0]


class TestExcludedCount:
    """Excluded violation count in summary output."""

    def test_no_violations_with_excluded(self):
        result = report([], excluded=42)
        assert "No violations found." in result
        assert "42 skipped via config" in result

    def test_no_violations_without_excluded(self):
        result = report([])
        assert "No violations found." in result
        assert "skipped" not in result

    def test_violations_with_excluded(self, make_violation):
        result = report([make_violation()], excluded=10)
        assert "10 skipped via config" in result

    def test_violations_without_excluded(self, make_violation):
        result = report([make_violation()])
        assert "skipped" not in result
