"""Reporter: text, JSON, and GitHub Actions annotation output."""

from __future__ import annotations

import json
from collections import defaultdict

import click

from dbt_lint.models import Violation

_SEVERITY_COLORS = {"error": "red", "warn": "yellow"}


def report(
    violations: list[Violation],
    output_format: str = "text",
    github_annotations: bool = False,
    excluded: int = 0,
    color: bool = False,
) -> str:
    """Format violations for output.

    Args:
        violations: List of violations to report.
        output_format: Output format ("text", "concise", "grouped", or "json").
        github_annotations: If True, also emit ::error/::warning lines.
        excluded: Number of violations suppressed via config.
        color: If True, apply ANSI color to severity tags and headers.

    Returns:
        Formatted string.
    """
    if output_format == "json":
        return _format_json(violations)

    parts: list[str] = []

    if github_annotations:
        parts.append(_format_annotations(violations))

    if output_format == "concise":
        parts.append(_format_concise(violations, excluded=excluded, color=color))
    elif output_format == "grouped":
        parts.append(_format_grouped(violations, excluded=excluded, color=color))
    else:
        parts.append(_format_text(violations, excluded=excluded, color=color))
    return "\n".join(parts)


def _style_severity(severity: str, *, color: bool) -> str:
    tag = f"[{severity}]"
    if not color:
        return tag
    return click.style(tag, fg=_SEVERITY_COLORS.get(severity))


def _summary(violations: list[Violation], excluded: int) -> str:
    """Build the 'Found N violations' summary line."""
    error_count = sum(1 for v in violations if v.severity == "error")
    warn_count = sum(1 for v in violations if v.severity == "warn")
    parts = []
    if error_count:
        parts.append(f"{error_count} error{'s' if error_count != 1 else ''}")
    if warn_count:
        parts.append(f"{warn_count} warning{'s' if warn_count != 1 else ''}")
    total = len(violations)
    suffix = "s" if total != 1 else ""
    line = f"\nFound {total} violation{suffix}: {', '.join(parts)}"
    if excluded:
        line += f"\n  {excluded} skipped via config"
    return line


def _format_text(
    violations: list[Violation], *, excluded: int = 0, color: bool = False
) -> str:
    if not violations:
        if excluded:
            return f"No violations found. ({excluded} skipped via config)"
        return "No violations found."

    by_category: dict[str, dict[str, list[Violation]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for v in violations:
        category = v.rule_id.split("/")[0] if "/" in v.rule_id else v.rule_id
        by_category[category][v.rule_id].append(v)

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
            for v in rule_violations:
                severity_tag = _style_severity(v.severity, color=color)
                lines.append(f"    {severity_tag} {v.message}")
                if v.file_path:
                    lines.append(f"            --> {v.file_path}")
                if v.patch_path:
                    lines.append(f"            yml: {v.patch_path}")

    lines.append(_summary(violations, excluded))
    category_counts = ", ".join(
        f"{cat} ({sum(len(rules) for rules in by_category[cat].values())})"
        for cat in sorted(by_category)
    )
    lines.append(f"  {category_counts}")

    return "\n".join(lines)


def _format_concise(
    violations: list[Violation], *, excluded: int = 0, color: bool = False
) -> str:
    if not violations:
        if excluded:
            return f"No violations found. ({excluded} skipped via config)"
        return "No violations found."

    lines: list[str] = []
    for v in violations:
        path = v.file_path or "(no file)"
        severity_tag = _style_severity(v.severity, color=color)
        lines.append(f"{path}: {severity_tag} {v.rule_id}: {v.message}")

    lines.append(_summary(violations, excluded))
    return "\n".join(lines)


def _format_grouped(
    violations: list[Violation], *, excluded: int = 0, color: bool = False
) -> str:
    if not violations:
        if excluded:
            return f"No violations found. ({excluded} skipped via config)"
        return "No violations found."

    by_file: dict[str, list[Violation]] = defaultdict(list)
    for v in violations:
        key = v.file_path or "(no file)"
        by_file[key].append(v)

    lines: list[str] = []
    for path in sorted(by_file):
        file_header = click.style(path, bold=True) if color else path
        lines.append(file_header)
        for v in by_file[path]:
            severity_tag = _style_severity(v.severity, color=color)
            lines.append(f"  {severity_tag} {v.rule_id}: {v.message}")
        lines.append("")

    lines.append(_summary(violations, excluded).lstrip("\n"))
    return "\n".join(lines)


def _format_json(violations: list[Violation]) -> str:
    return json.dumps(
        [
            {
                "rule_id": v.rule_id,
                "resource_id": v.resource_id,
                "resource_name": v.resource_name,
                "message": v.message,
                "severity": v.severity,
                "file_path": v.file_path,
                "patch_path": v.patch_path,
            }
            for v in violations
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
    for v in violations:
        level = "error" if v.severity == "error" else "warning"
        file_path = _escape_annotation(v.file_path)
        title = _escape_annotation(v.rule_id)
        message = _escape_annotation(v.message)
        lines.append(f"::{level} file={file_path},title={title}::{message}")
    return "\n".join(lines)
