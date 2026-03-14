"""Tests for reporter module: text, JSON, and GitHub Actions annotation output."""

from __future__ import annotations

import json

from dbt_linter.models import Violation
from dbt_linter.reporter import report


def _v(
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
        result = report([], format="text")
        assert "no violations" in result.lower()

    def test_single_violation(self):
        violations = [_v()]
        result = report(violations, format="text")
        assert "documentation" in result.lower()
        assert "undocumented-models" in result
        assert "stg_users" in result

    def test_groups_by_category(self):
        violations = [
            _v(rule_id="documentation/undocumented-models", resource_name="m1"),
            _v(
                rule_id="governance/public-models-without-contract",
                resource_name="m2",
                message="m2: public without contract",
            ),
        ]
        result = report(violations, format="text")
        # Both categories should appear as section headers
        assert "documentation" in result.lower()
        assert "governance" in result.lower()

    def test_groups_by_rule_within_category(self):
        violations = [
            _v(
                rule_id="documentation/undocumented-models",
                resource_name="m1",
                message="m1: missing description",
            ),
            _v(
                rule_id="documentation/documentation-coverage",
                resource_name="m2",
                message="m2: below coverage target",
                resource_id="model.pkg.m2",
            ),
            _v(
                rule_id="documentation/undocumented-models",
                resource_name="m3",
                message="m3: missing description",
                resource_id="model.pkg.m3",
            ),
        ]
        result = report(violations, format="text")
        lines = result.split("\n")
        # Both rule IDs from documentation category should appear
        assert any("undocumented-models" in line for line in lines)
        assert any("documentation-coverage" in line for line in lines)

    def test_severity_shown_for_errors(self):
        violations = [_v(severity="error")]
        result = report(violations, format="text")
        assert "error" in result.lower()

    def test_summary_line_with_counts(self):
        violations = [
            _v(severity="warn"),
            _v(
                severity="error",
                resource_name="m2",
                resource_id="model.pkg.m2",
                message="m2: missing description",
            ),
        ]
        result = report(violations, format="text")
        # Should have a summary with total count
        assert "2" in result

    def test_multiple_violations_same_rule(self):
        violations = [
            _v(resource_name="m1", message="m1: missing description"),
            _v(
                resource_name="m2",
                resource_id="model.pkg.m2",
                message="m2: missing description",
            ),
        ]
        result = report(violations, format="text")
        assert "m1" in result
        assert "m2" in result


class TestJsonReport:
    """JSON output: list of violation objects."""

    def test_empty_violations_returns_empty_list(self):
        result = report([], format="json")
        assert json.loads(result) == []

    def test_single_violation_structure(self):
        violations = [_v()]
        result = json.loads(report(violations, format="json"))
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
            _v(resource_name="m1"),
            _v(resource_name="m2", resource_id="model.pkg.m2"),
        ]
        result = json.loads(report(violations, format="json"))
        assert len(result) == 2
        names = {obj["resource_name"] for obj in result}
        assert names == {"m1", "m2"}

    def test_json_is_valid(self):
        violations = [_v(message='has "quotes" and\nnewlines')]
        result = report(violations, format="json")
        parsed = json.loads(result)
        assert parsed[0]["message"] == 'has "quotes" and\nnewlines'


class TestGitHubAnnotations:
    """GitHub Actions ::error/::warning workflow commands."""

    def test_warning_annotation_format(self):
        violations = [_v(severity="warn")]
        result = report(violations, format="text", github_annotations=True)
        assert "::warning file=models/staging/stg_users.sql" in result
        assert "title=documentation/undocumented-models" in result
        assert "::stg_users: missing description" in result

    def test_error_annotation_format(self):
        violations = [_v(severity="error")]
        result = report(violations, format="text", github_annotations=True)
        assert "::error file=models/staging/stg_users.sql" in result

    def test_multiple_annotations(self):
        violations = [
            _v(severity="warn", resource_name="m1", message="m1: issue"),
            _v(
                severity="error",
                resource_name="m2",
                resource_id="model.pkg.m2",
                message="m2: issue",
            ),
        ]
        result = report(violations, format="text", github_annotations=True)
        assert result.count("::warning") == 1
        assert result.count("::error") == 1

    def test_annotations_precede_text_output(self):
        violations = [_v()]
        result = report(violations, format="text", github_annotations=True)
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
        violations = [_v()]
        result = report(violations, format="text", github_annotations=False)
        assert "::" not in result or "::warning" not in result

    def test_empty_violations_no_annotations(self):
        result = report([], format="text", github_annotations=True)
        assert "::" not in result
