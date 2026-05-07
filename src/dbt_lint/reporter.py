"""Reporter: text, concise, grouped, JSON, and GitHub Actions output."""

from __future__ import annotations

import json
from collections import defaultdict

import click

from dbt_lint.models import Violation

_SEVERITY_COLORS = {"error": "red", "warn": "yellow"}


def report(  # noqa: PLR0913
    violations: list[Violation],
    output_format: str = "text",
    github_annotations: bool = False,
    excluded: int = 0,
    color: bool = False,
    resource_counts: dict[str, int] | None = None,
) -> str:
    """Format violations for output.

    Args:
        violations: List of violations to report.
        output_format: Output format ("text", "concise", "grouped", or "json").
        github_annotations: If True, also emit ::error/::warning lines.
        excluded: Number of violations suppressed via config.
        color: If True, apply ANSI color to severity tags and headers.
        resource_counts: Per-resource-type counts. When present and non-empty,
            the non-JSON summary line is prefixed with "Inspected N models, ...".

    Returns:
        Formatted string.
    """
    if output_format == "json":
        return _format_json(violations)

    parts: list[str] = []

    if github_annotations:
        parts.append(_format_annotations(violations))

    summary = _unified_summary(violations, excluded, resource_counts)

    if output_format == "concise":
        parts.append(_format_concise(violations, summary=summary, color=color))
    elif output_format == "grouped":
        parts.append(_format_grouped(violations, summary=summary, color=color))
    else:
        parts.append(_format_text(violations, summary=summary, color=color))
    return "\n".join(parts)


def _unified_summary(
    violations: list[Violation],
    excluded: int,
    resource_counts: dict[str, int] | None,
) -> str:
    """Build the single-line summary: optional scope prefix + findings."""
    findings = _format_findings(violations, excluded)
    scope = _format_scope(resource_counts)
    if scope:
        return f"Inspected {scope}. {findings}"
    return findings


def _format_scope(resource_counts: dict[str, int] | None) -> str:
    """Build "1034 models, 542 sources" from non-zero counts; "" when empty."""
    if not resource_counts:
        return ""
    pieces = [
        f"{count} {resource_type}{'s' if count != 1 else ''}"
        for resource_type, count in sorted(resource_counts.items())
        if count > 0
    ]
    return ", ".join(pieces)


def _format_findings(violations: list[Violation], excluded: int) -> str:
    """Build "Found 5 violations (2 errors, 3 warnings, 553 skipped)" line.

    When all violations share one severity, the headline names the severity
    directly ("Found 5 warnings") and the parenthetical breakdown is omitted.
    """
    error_count = sum(1 for violation in violations if violation.severity == "error")
    warn_count = sum(1 for violation in violations if violation.severity == "warn")
    total = len(violations)

    headline = _findings_headline(total, error_count, warn_count)

    paren_parts: list[str] = []
    if error_count and warn_count:
        paren_parts.append(_pluralize(error_count, "error"))
        paren_parts.append(_pluralize(warn_count, "warning"))
    if excluded:
        paren_parts.append(f"{excluded} skipped")

    if paren_parts:
        return f"Found {headline} ({', '.join(paren_parts)})"
    return f"Found {headline}"


def _findings_headline(total: int, error_count: int, warn_count: int) -> str:
    if total == 0:
        return "0 violations"
    if error_count and not warn_count:
        return _pluralize(error_count, "error")
    if warn_count and not error_count:
        return _pluralize(warn_count, "warning")
    return _pluralize(total, "violation")


def _pluralize(count: int, noun: str) -> str:
    return f"{count} {noun}{'s' if count != 1 else ''}"


def _style_severity(severity: str, *, color: bool) -> str:
    tag = f"[{severity}]"
    if not color:
        return tag
    return click.style(tag, fg=_SEVERITY_COLORS.get(severity))


def _format_text(
    violations: list[Violation], *, summary: str, color: bool = False
) -> str:
    if not violations:
        return summary

    by_category: dict[str, dict[str, list[Violation]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for violation in violations:
        category = (
            violation.rule_id.split("/")[0]
            if "/" in violation.rule_id
            else violation.rule_id
        )
        by_category[category][violation.rule_id].append(violation)

    lines: list[str] = []
    for category in sorted(by_category):
        cat_header = click.style(category, bold=True) if color else category
        lines.append(f"\n{cat_header}")
        lines.append("=" * len(category))
        for rule_id in sorted(by_category[category]):
            rule_violations = by_category[category][rule_id]
            rule_name = rule_id.split("/")[1] if "/" in rule_id else rule_id
            header = f"{rule_name} ({len(rule_violations)})"
            lines.append(f"\n  {header}")
            lines.append(f"  {'-' * len(header)}")
            for violation in rule_violations:
                severity_tag = _style_severity(violation.severity, color=color)
                lines.append(f"    {severity_tag} {violation.message}")
                if violation.file_path:
                    lines.append(f"            --> {violation.file_path}")
                if violation.patch_path:
                    lines.append(f"            yml: {violation.patch_path}")

    lines.append("")
    lines.append(summary)
    return "\n".join(lines)


def _format_concise(
    violations: list[Violation], *, summary: str, color: bool = False
) -> str:
    if not violations:
        return summary

    lines: list[str] = []
    for violation in violations:
        path = violation.file_path or "(no file)"
        severity_tag = _style_severity(violation.severity, color=color)
        lines.append(f"{path}: {severity_tag} {violation.rule_id}: {violation.message}")

    lines.append("")
    lines.append(summary)
    return "\n".join(lines)


def _format_grouped(
    violations: list[Violation], *, summary: str, color: bool = False
) -> str:
    if not violations:
        return summary

    by_file: dict[str, list[Violation]] = defaultdict(list)
    for violation in violations:
        key = violation.file_path or "(no file)"
        by_file[key].append(violation)

    lines: list[str] = []
    for path in sorted(by_file):
        file_header = click.style(path, bold=True) if color else path
        lines.append(file_header)
        for violation in by_file[path]:
            severity_tag = _style_severity(violation.severity, color=color)
            lines.append(f"  {severity_tag} {violation.rule_id}: {violation.message}")
        lines.append("")

    lines.append(summary)
    return "\n".join(lines)


def _format_json(violations: list[Violation]) -> str:
    return json.dumps(
        [
            {
                "rule_id": violation.rule_id,
                "resource_id": violation.resource_id,
                "resource_name": violation.resource_name,
                "message": violation.message,
                "severity": violation.severity,
                "file_path": violation.file_path,
                "patch_path": violation.patch_path,
            }
            for violation in violations
        ],
        indent=2,
    )


def _escape_annotation(value: str) -> str:
    """Escape special characters for GitHub Actions workflow commands."""
    return (
        value.replace("%", "%25")
        .replace("\n", "%0A")
        .replace("\r", "%0D")
        .replace(",", "%2C")
        .replace(":", "%3A")
    )


def _format_annotations(violations: list[Violation]) -> str:
    lines: list[str] = []
    for violation in violations:
        level = "error" if violation.severity == "error" else "warning"
        file_path = _escape_annotation(violation.file_path)
        title = _escape_annotation(violation.rule_id)
        message = _escape_annotation(violation.message)
        lines.append(f"::{level} file={file_path},title={title}::{message}")
    return "\n".join(lines)
