"""CLI entry point: dbt-lint <manifest.json> [options]."""

from __future__ import annotations

import dataclasses
import json
import os
import sys
from itertools import groupby
from pathlib import Path

import click

from dbt_lint._lint import LintError, LintResult, run
from dbt_lint.config import SUPPRESSIONS_FILENAME
from dbt_lint.models import Violation
from dbt_lint.reporter import report
from dbt_lint.rules import generate_rules_index
from dbt_lint.suppressions import generate_suppressions


def _handle_list_rules(output_format: str) -> None:
    """Print the rules index and exit."""
    index = generate_rules_index()

    if output_format == "json":
        click.echo(json.dumps([dataclasses.asdict(rule) for rule in index], indent=2))
        return

    for category, rules in groupby(index, key=lambda rule: rule.category):
        click.echo(f"\n{category}")
        for rule in rules:
            click.echo(f"  {rule.id}: {rule.description}")

    categories = {rule.category for rule in index}
    click.echo(f"\n{len(index)} rules across {len(categories)} categories")


def _resolve_suppressions(
    explicit: Path | None,
    config_path: Path | None,
) -> Path | None:
    """Find the suppressions file: explicit path, or auto-discover by convention."""
    if explicit is not None:
        return explicit
    search_dir = config_path.parent if config_path is not None else Path.cwd()
    candidate = search_dir / SUPPRESSIONS_FILENAME
    return candidate if candidate.exists() else None


def _determine_exit_code(violations: list[Violation], fail_on: str) -> int:
    """Return 1 if any violation meets the fail_on threshold, else 0."""
    if fail_on == "error":
        return (
            1 if any(violation.severity == "error" for violation in violations) else 0
        )
    return 1 if violations else 0


def _emit_suppressions(violations: list[Violation], output_path: Path | None) -> None:
    """Write a generated suppressions YAML to file or stdout."""
    suppressions = generate_suppressions(violations)
    if output_path:
        output_path.write_text(suppressions)
    else:
        click.echo(suppressions, nl=False)


def _emit_report(result: LintResult, output_format: str) -> None:
    """Render the lint result to stdout."""
    github_annotations = os.environ.get("GITHUB_ACTIONS") == "true"
    use_color = (
        output_format != "json"
        and os.environ.get("NO_COLOR") is None
        and click.get_text_stream("stdout").isatty()
    )
    output = report(
        result.violations,
        output_format=output_format,
        github_annotations=github_annotations,
        excluded=result.excluded,
        color=use_color,
        resource_counts=result.resource_counts,
    )
    click.echo(output)


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
    help="Path to dbt_lint.yml config file.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "concise", "grouped", "json"]),
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
    "--suppressions",
    "suppressions_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to .dbt-lint-suppressions.yml file.",
)
@click.option(
    "--list-rules",
    "list_rules",
    is_flag=True,
    default=False,
    help="List all available rules and exit.",
)
@click.option(
    "--write-suppressions",
    "write_suppressions_flag",
    is_flag=True,
    default=False,
    help="Output a YAML config that suppresses all current violations.",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Write output to file instead of stdout (use with --write-suppressions).",
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
    suppressions_path: Path | None,
    write_suppressions_flag: bool,
    output_path: Path | None,
) -> None:
    """Lint a dbt project by analyzing its manifest.json."""
    if list_rules:
        _handle_list_rules(output_format)
        return

    if manifest is None:
        raise click.UsageError("Missing argument 'MANIFEST'.")

    resolved_suppressions = (
        None
        if write_suppressions_flag
        else _resolve_suppressions(suppressions_path, config)
    )

    try:
        result = run(
            manifest_path=manifest,
            config_path=config,
            suppressions_path=resolved_suppressions,
            select=select,
            exclude=exclude,
            fail_fast=fail_fast,
        )
    except LintError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)

    if write_suppressions_flag:
        _emit_suppressions(result.violations, output_path)
        sys.exit(0)

    _emit_report(result, output_format)
    sys.exit(_determine_exit_code(result.violations, fail_on))


if __name__ == "__main__":
    main()
