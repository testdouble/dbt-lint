"""CLI entry point: dbt-lint <manifest.json> [options]."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click

from dbt_linter.baseline import generate_baseline
from dbt_linter.config import load_config
from dbt_linter.engine import evaluate
from dbt_linter.graph import build_relationships
from dbt_linter.manifest import parse_manifest
from dbt_linter.models import Violation
from dbt_linter.reporter import report


def _apply_filters(
    violations: list[Violation],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
) -> list[Violation]:
    """Filter violations by --select and --exclude rule IDs."""
    if select:
        violations = [v for v in violations if v.rule_id in select]
    if exclude:
        violations = [v for v in violations if v.rule_id not in exclude]
    return violations


def _determine_exit_code(violations: list[Violation], fail_on: str) -> int:
    """Return 1 if any violation meets the fail_on threshold, else 0."""
    if fail_on == "error":
        has_blocking = any(v.severity == "error" for v in violations)
    else:
        has_blocking = len(violations) > 0
    return 1 if has_blocking else 0


@click.command()
@click.argument("manifest", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to dbt_linter.yml config file.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format.",
)
@click.option(
    "--select",
    multiple=True,
    help="Only run these rule IDs.",
)
@click.option(
    "--exclude",
    multiple=True,
    help="Skip these rule IDs.",
)
@click.option(
    "--fail-on",
    "fail_on",
    type=click.Choice(["warn", "error"]),
    default="warn",
    help="Minimum severity that causes exit code 1.",
)
@click.option(
    "--fail-fast",
    is_flag=True,
    default=False,
    help="Stop after the first violation.",
)
@click.option(
    "--generate-baseline",
    "generate_baseline_flag",
    is_flag=True,
    default=False,
    help="Output a YAML config that suppresses all current violations.",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Write output to file instead of stdout (use with --generate-baseline).",
)
def main(
    manifest: Path,
    config: Path | None,
    output_format: str,
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    fail_on: str,
    fail_fast: bool,
    generate_baseline_flag: bool,
    output_path: Path | None,
) -> None:
    """Lint a dbt project by analyzing its manifest.json."""
    try:
        cfg = load_config(config)
        resources, edges = parse_manifest(manifest, cfg)
        relationships = build_relationships(resources, edges)
        violations = evaluate(resources, relationships, cfg, fail_fast=fail_fast)
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)

    violations = _apply_filters(violations, select, exclude)

    if generate_baseline_flag:
        baseline = generate_baseline(violations)
        if output_path:
            output_path.write_text(baseline)
        else:
            click.echo(baseline, nl=False)
        sys.exit(0)

    github_annotations = os.environ.get("GITHUB_ACTIONS") == "true"
    output = report(
        violations,
        format=output_format,
        github_annotations=github_annotations,
    )
    click.echo(output)

    sys.exit(_determine_exit_code(violations, fail_on))


if __name__ == "__main__":
    main()
