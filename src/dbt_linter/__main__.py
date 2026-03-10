"""CLI entry point: dbt-lint <manifest.json> [options]."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click

from dbt_linter.config import load_config
from dbt_linter.engine import evaluate
from dbt_linter.graph import build_relationships
from dbt_linter.manifest import parse_manifest
from dbt_linter.reporter import report


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
def main(
    manifest: Path,
    config: Path | None,
    output_format: str,
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    fail_on: str,
) -> None:
    """Lint a dbt project by analyzing its manifest.json."""
    try:
        cfg = load_config(config)
        resources, edges = parse_manifest(manifest, cfg)
        relationships = build_relationships(resources, edges)
        violations = evaluate(resources, relationships, cfg)
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)

    # Apply --select / --exclude filters
    if select:
        violations = [v for v in violations if v.rule_id in select]
    if exclude:
        violations = [v for v in violations if v.rule_id not in exclude]

    github_annotations = os.environ.get("GITHUB_ACTIONS") == "true"
    output = report(
        violations,
        format=output_format,
        github_annotations=github_annotations,
    )
    click.echo(output)

    # Determine exit code
    if fail_on == "error":
        has_blocking = any(v.severity == "error" for v in violations)
    else:
        has_blocking = len(violations) > 0

    sys.exit(1 if has_blocking else 0)


if __name__ == "__main__":
    main()
