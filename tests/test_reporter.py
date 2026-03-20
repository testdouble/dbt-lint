"""Tests for reporter module: text, JSON, and GitHub Actions annotation output."""

from __future__ import annotations

import json

from dbt_linter.models import Violation
from dbt_linter.reporter import report


def _violation(
    rule_id: str = "documentation/undocumented-models",
    resource_id: str = "model.pkg.stg_users",
    resource_name: str = "stg_users",
    message: str = "stg_users: missing description",
    severity: str = "warn",
    file_path: str = "models/staging/stg_users.sql",
) -> Violation:
    return Violation(
        rule_id=rule_id,
        resource_id=resource_id,
        resource_name=resource_name,
        message=message,
        severity=severity,
        file_path=file_path,
    )


class TestTextReport:
    """Text output: grouped by category, then rule."""

    def test_empty_violations_returns_clean_message(self):
        result = report([], output_format="text")
        assert "no violations" in result.lower()

    def test_single_violation(self):
        violations = [_violation()]
        result = report(violations, output_format="text")
        assert "documentation" in result.lower()
        assert "undocumented-models" in result
        assert "stg_users" in result

    def test_groups_by_category(self):
        violations = [
            _violation(rule_id="documentation/undocumented-models", resource_name="m1"),
            _violation(
                rule_id="governance/public-models-without-contract",
                resource_name="m2",
                message="m2: public without contract",
            ),
        ]
        result = report(violations, output_format="text")
        # Both categories should appear as section headers
        assert "documentation" in result.lower()
        assert "governance" in result.lower()

    def test_groups_by_rule_within_category(self):
        violations = [
            _violation(
                rule_id="documentation/undocumented-models",
                resource_name="m1",
                message="m1: missing description",
            ),
            _violation(
                rule_id="documentation/documentation-coverage",
                resource_name="m2",
                message="m2: below coverage target",
                resource_id="model.pkg.m2",
            ),
            _violation(
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

    def test_severity_shown_for_errors(self):
        violations = [_violation(severity="error")]
        result = report(violations, output_format="text")
        assert "error" in result.lower()

    def test_summary_line_with_counts(self):
        violations = [
            _violation(severity="warn"),
            _violation(
                severity="error",
                resource_name="m2",
                resource_id="model.pkg.m2",
                message="m2: missing description",
            ),
        ]
        result = report(violations, output_format="text")
        assert "Found 2 violations" in result

    def test_summary_includes_category_breakdown(self):
        violations = [
            _violation(rule_id="documentation/undocumented-models", resource_name="m1"),
            _violation(
                rule_id="governance/public-models-without-contract",
                resource_name="m2",
                message="m2: public without contract",
            ),
            _violation(
                rule_id="documentation/documentation-coverage",
                resource_name="m3",
                message="m3: below coverage target",
                resource_id="model.pkg.m3",
            ),
        ]
        result = report(violations, output_format="text")
        assert "documentation (2)" in result
        assert "governance (1)" in result

    def test_multiple_violations_same_rule(self):
        violations = [
            _violation(resource_name="m1", message="m1: missing description"),
            _violation(
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
        assert json.loads(result) == []

    def test_single_violation_structure(self):
        violations = [_violation()]
        result = json.loads(report(violations, output_format="json"))
        assert len(result) == 1
        obj = result[0]
        assert obj["rule_id"] == "documentation/undocumented-models"
        assert obj["resource_id"] == "model.pkg.stg_users"
        assert obj["resource_name"] == "stg_users"
        assert obj["message"] == "stg_users: missing description"
        assert obj["severity"] == "warn"
        assert obj["file_path"] == "models/staging/stg_users.sql"

    def test_multiple_violations(self):
        violations = [
            _violation(resource_name="m1"),
            _violation(resource_name="m2", resource_id="model.pkg.m2"),
        ]
        result = json.loads(report(violations, output_format="json"))
        assert len(result) == 2
        names = {obj["resource_name"] for obj in result}
        assert names == {"m1", "m2"}

    def test_json_roundtrips_special_characters(self):
        violations = [_violation(message='has "quotes" and\nnewlines')]
        result = report(violations, output_format="json")
        parsed = json.loads(result)
        assert len(parsed) == 1
        assert parsed[0]["message"] == 'has "quotes" and\nnewlines'


class TestGitHubAnnotations:
    """GitHub Actions ::error/::warning workflow commands."""

    def test_warning_annotation_format(self):
        violations = [_violation(severity="warn")]
        result = report(violations, output_format="text", github_annotations=True)
        assert "::warning file=models/staging/stg_users.sql" in result
        assert "title=documentation/undocumented-models" in result
        assert "::stg_users: missing description" in result

    def test_error_annotation_format(self):
        violations = [_violation(severity="error")]
        result = report(violations, output_format="text", github_annotations=True)
        assert "::error file=models/staging/stg_users.sql" in result

    def test_multiple_annotations(self):
        violations = [
            _violation(severity="warn", resource_name="m1", message="m1: issue"),
            _violation(
                severity="error",
                resource_name="m2",
                resource_id="model.pkg.m2",
                message="m2: issue",
            ),
        ]
        result = report(violations, output_format="text", github_annotations=True)
        assert result.count("::warning") == 1
        assert result.count("::error") == 1

    def test_annotations_precede_text_output(self):
        violations = [_violation()]
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

    def test_no_annotations_without_flag(self):
        violations = [_violation()]
        result = report(violations, output_format="text", github_annotations=False)
        assert "::" not in result

    def test_empty_violations_no_annotations(self):
        result = report([], output_format="text", github_annotations=True)
        assert "::" not in result


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

    def test_violations_with_excluded(self):
        result = report([_violation()], excluded=10)
        assert "10 skipped via config" in result

    def test_violations_without_excluded(self):
        result = report([_violation()])
        assert "skipped" not in result
