"""Reporter: text, JSON, and GitHub Actions annotation output."""

from __future__ import annotations

import json
from collections import defaultdict

from dbt_linter.models import Violation


def report(
    violations: list[Violation],
    format: str = "text",
    github_annotations: bool = False,
    excluded: int = 0,
) -> str:
    """Format violations for output.

    Args:
        violations: List of violations to report.
        format: Output format ("text" or "json").
        github_annotations: If True, also emit ::error/::warning lines.
        excluded: Number of violations suppressed via config.

    Returns:
        Formatted string.
    """
    if format == "json":
        return _format_json(violations)

    parts: list[str] = []

    if github_annotations:
        parts.append(_format_annotations(violations))

    parts.append(_format_text(violations, excluded=excluded))
    return "\n".join(parts)


def _format_text(violations: list[Violation], *, excluded: int = 0) -> str:
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
        lines.append(f"\n{category}")
        lines.append("=" * len(category))
        for rule_id in sorted(by_category[category]):
            rule_violations = by_category[category][rule_id]
            rule_name = rule_id.split("/")[1] if "/" in rule_id else rule_id
            header = f"{rule_name} ({len(rule_violations)})"
            lines.append(f"\n  {header}")
            lines.append(f"  {'-' * len(header)}")
            for v in rule_violations:
                severity_tag = f" [{v.severity}]" if v.severity == "error" else ""
                lines.append(f"    {v.message}{severity_tag}")

    # Summary
    error_count = sum(1 for v in violations if v.severity == "error")
    warn_count = sum(1 for v in violations if v.severity == "warn")
    summary_parts = []
    if error_count:
        summary_parts.append(f"{error_count} error{'s' if error_count != 1 else ''}")
    if warn_count:
        summary_parts.append(f"{warn_count} warning{'s' if warn_count != 1 else ''}")
    total = len(violations)
    suffix = "s" if total != 1 else ""
    detail = ", ".join(summary_parts)
    lines.append(f"\nFound {total} violation{suffix}: {detail}")
    category_counts = ", ".join(
        f"{cat} ({sum(len(rules) for rules in by_category[cat].values())})"
        for cat in sorted(by_category)
    )
    lines.append(f"  {category_counts}")
    if excluded:
        lines.append(f"  {excluded} skipped via config")

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
            }
            for v in violations
        ],
        indent=2,
    )


def _format_annotations(violations: list[Violation]) -> str:
    lines: list[str] = []
    for v in violations:
        level = "error" if v.severity == "error" else "warning"
        lines.append(f"::{level} file={v.file_path},title={v.rule_id}::{v.message}")
    return "\n".join(lines)
