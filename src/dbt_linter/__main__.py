"""CLI entry point: dbt-lint <manifest.json> [options]."""

from __future__ import annotations

import dataclasses
import json
import os
import sys
from itertools import groupby
from pathlib import Path

import click

from dbt_linter.baseline import generate_baseline
from dbt_linter.config import (
    BASELINE_FILENAME,
    load_baseline,
    load_config,
    merge_baseline,
)
from dbt_linter.engine import evaluate
from dbt_linter.graph import build_relationships
from dbt_linter.manifest import parse_manifest
from dbt_linter.models import Violation
from dbt_linter.reporter import report
from dbt_linter.rules import generate_rules_index


def _handle_list_rules(output_format: str) -> None:
    """Print the rules index and exit."""
    index = generate_rules_index()

    if output_format == "json":
        click.echo(json.dumps([dataclasses.asdict(r) for r in index], indent=2))
        return

    for category, rules in groupby(index, key=lambda r: r.category):
        click.echo(f"\n{category}")
        for r in rules:
            click.echo(f"  {r.id}: {r.description}")

    categories = {r.category for r in index}
    click.echo(f"\n{len(index)} rules across {len(categories)} categories")


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


def _resolve_baseline(
    explicit: Path | None,
    config_path: Path | None,
) -> Path | None:
    """Find the baseline file: explicit path, or auto-discover by convention."""
    if explicit is not None:
        return explicit
    search_dir = config_path.parent if config_path is not None else Path.cwd()
    candidate = search_dir / BASELINE_FILENAME
    return candidate if candidate.exists() else None


def _determine_exit_code(violations: list[Violation], fail_on: str) -> int:
    """Return 1 if any violation meets the fail_on threshold, else 0."""
    if fail_on == "error":
        return 1 if any(v.severity == "error" for v in violations) else 0
    return 1 if violations else 0


@click.command()
@click.argument(
    "manifest",
    type=click.Path(exists=True, path_type=Path),
    required=False,
)
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
    "--baseline",
    "baseline_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to dbt-lint-baseline.yml suppressions file.",
)
@click.option(
    "--list-rules",
    "list_rules",
    is_flag=True,
    default=False,
    help="List all available rules and exit.",
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
def main(  # noqa: PLR0913
    manifest: Path | None,
    config: Path | None,
    output_format: str,
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    fail_on: str,
    fail_fast: bool,
    list_rules: bool,
    baseline_path: Path | None,
    generate_baseline_flag: bool,
    output_path: Path | None,
) -> None:
    """Lint a dbt project by analyzing its manifest.json."""
    if list_rules:
        _handle_list_rules(output_format)
        return

    if manifest is None:
        raise click.UsageError("Missing argument 'MANIFEST'.")

    try:
        cfg = load_config(config)
        if not generate_baseline_flag:
            resolved = _resolve_baseline(baseline_path, config)
            if resolved is not None:
                cfg = merge_baseline(cfg, load_baseline(resolved))
        resources, edges = parse_manifest(manifest, cfg)
        relationships = build_relationships(resources, edges)
        result = evaluate(resources, relationships, cfg, fail_fast=fail_fast)
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)

    violations = _apply_filters(result.violations, select, exclude)

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
        output_format=output_format,
        github_annotations=github_annotations,
        excluded=result.excluded,
    )
    click.echo(output)

    sys.exit(_determine_exit_code(violations, fail_on))


if __name__ == "__main__":
    main()
