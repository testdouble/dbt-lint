"""CLI entry point: dbt-lint check / dbt-lint rule subcommands."""

from __future__ import annotations

import dataclasses
import json
import os
import sys
from itertools import groupby
from pathlib import Path

import click
import yaml

from dbt_lint._lint import (
    LintError,
    LintResult,
    UnknownRuleError,
    collect_rules,
    run,
)
from dbt_lint.config import (
    SUPPRESSIONS_FILENAME,
    discover_config_path,
    load_config,
)
from dbt_lint.models import Violation
from dbt_lint.reporter import report
from dbt_lint.rules import RuleInfo, build_rule_index
from dbt_lint.suppressions import generate_suppressions

CHECK_OUTPUT_FORMATS = ["text", "concise", "grouped", "json"]
RULE_OUTPUT_FORMATS = ["text", "json"]
EXAMPLE_USAGE = (
    "\b\nExamples:\n  dbt-lint check target/manifest.json\n  dbt-lint rule --all"
)


def _resolve_suppressions(
    explicit: Path | None,
    config_dir: Path | None,
    *,
    isolated: bool,
    skip_auto_load: bool,
) -> Path | None:
    """Find the suppressions file: explicit path, or auto-discover by convention.

    Auto-discovery checks the discovered config's directory and cwd. ``isolated``
    or ``skip_auto_load`` (set by --write-suppressions) skip auto-discovery while
    still honoring an explicit path.
    """
    if explicit is not None:
        return explicit
    if isolated or skip_auto_load:
        return None
    search_dirs: list[Path] = []
    if config_dir is not None:
        search_dirs.append(config_dir)
    cwd = Path.cwd()
    if cwd not in search_dirs:
        search_dirs.append(cwd)
    for directory in search_dirs:
        candidate = directory / SUPPRESSIONS_FILENAME
        if candidate.exists():
            return candidate
    return None


def _determine_exit_code(
    violations: list[Violation], fail_on: str, *, exit_zero: bool
) -> int:
    """Return 0 when ``--exit-zero`` is set, else 1 if any violation hits the
    ``fail_on`` threshold."""
    if exit_zero:
        return 0
    if fail_on == "error":
        return (
            1 if any(violation.severity == "error" for violation in violations) else 0
        )
    return 1 if violations else 0


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


@click.group(
    help=(
        "Static analysis for dbt projects.\n\n" + EXAMPLE_USAGE + "\n\n"
        "Run 'dbt-lint check --help' or 'dbt-lint rule --help' for"
        " subcommand options."
    )
)
@click.version_option(package_name="dbt-lint")
def main() -> None:
    """Top-level dbt-lint command group."""


@main.command(no_args_is_help=True)
@click.argument(
    "manifest",
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to dbt-lint.yml config file.",
)
@click.option(
    "--output-format",
    type=click.Choice(CHECK_OUTPUT_FORMATS),
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
    "--severity",
    type=click.Choice(["warn", "error"]),
    default=None,
    help="Minimum severity to report.",
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
    "--exit-zero",
    is_flag=True,
    default=False,
    help="Force exit code 0 regardless of violations found.",
)
@click.option(
    "--isolated",
    is_flag=True,
    default=False,
    help="Bypass config discovery and suppressions auto-load.",
)
@click.option(
    "--suppressions",
    "suppressions_path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to .dbt-lint-suppressions.yml file.",
)
@click.option(
    "--write-suppressions",
    "write_suppressions_flag",
    is_flag=True,
    default=False,
    help="Emit a YAML suppressions file from current violations to stdout.",
)
def check(  # noqa: PLR0913
    manifest: Path,
    config: Path | None,
    output_format: str,
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    severity: str | None,
    fail_on: str,
    fail_fast: bool,
    exit_zero: bool,
    isolated: bool,
    suppressions_path: Path | None,
    write_suppressions_flag: bool,
) -> None:
    """Lint a dbt project by analyzing its manifest.json."""
    discovered_config = config
    if discovered_config is None and not isolated:
        discovered_config = discover_config_path()
    config_dir = discovered_config.parent if discovered_config is not None else None

    resolved_suppressions = _resolve_suppressions(
        suppressions_path,
        config_dir,
        isolated=isolated,
        skip_auto_load=write_suppressions_flag,
    )

    try:
        result = run(
            manifest_path=manifest,
            config_path=config,
            suppressions_path=resolved_suppressions,
            select=select,
            exclude=exclude,
            fail_fast=fail_fast,
            severity=severity,
            isolated=isolated,
        )
    except LintError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)

    if write_suppressions_flag:
        click.echo(generate_suppressions(result.violations), nl=False)
        sys.exit(0)

    _emit_report(result, output_format)
    sys.exit(_determine_exit_code(result.violations, fail_on, exit_zero=exit_zero))


@main.command()
@click.argument("rule_id", required=False)
@click.option(
    "--all",
    "list_all",
    is_flag=True,
    default=False,
    help="List every rule.",
)
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to dbt-lint.yml config file.",
)
@click.option(
    "--isolated",
    is_flag=True,
    default=False,
    help="Bypass config discovery (skips custom-rule loading).",
)
@click.option(
    "--output-format",
    type=click.Choice(RULE_OUTPUT_FORMATS),
    default="text",
    help="Output format.",
)
@click.pass_context
def rule(  # noqa: PLR0913
    context: click.Context,
    rule_id: str | None,
    list_all: bool,
    config: Path | None,
    isolated: bool,
    output_format: str,
) -> None:
    """List or explain rules."""
    if not list_all and rule_id is None:
        click.echo(context.get_help())
        sys.exit(2)

    try:
        loaded_config = load_config(config, isolated=isolated)
    except (yaml.YAMLError, OSError, ValueError) as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)

    try:
        rules = collect_rules(loaded_config)
        index = build_rule_index(rules)
        if list_all:
            _emit_rules_index(index, output_format)
            return
        info = next((entry for entry in index if entry.id == rule_id), None)
        if info is None:
            raise UnknownRuleError(f"unknown rule '{rule_id}'")
    except LintError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)

    _emit_rule_explain(info, output_format)


def _emit_rules_index(index: list[RuleInfo], output_format: str) -> None:
    """Render the full rules index (text or JSON)."""
    if output_format == "json":
        click.echo(json.dumps([dataclasses.asdict(info) for info in index], indent=2))
        return

    for category, rules in groupby(index, key=lambda info: info.category):
        click.echo(f"\n{category}")
        for info in rules:
            click.echo(f"  {info.id}: {info.description}")

    categories = {info.category for info in index}
    click.echo(f"\n{len(index)} rules across {len(categories)} categories")


def _emit_rule_explain(info: RuleInfo, output_format: str) -> None:
    """Render a single rule's metadata. Empty sections are omitted."""
    if output_format == "json":
        click.echo(json.dumps(dataclasses.asdict(info), indent=2))
        return

    examples_block = "\n".join(f"- {example}" for example in info.examples)
    sections = [f"{info.id}: {info.description}"]
    for heading, body in (
        ("Rationale", info.rationale),
        ("Remediation", info.remediation),
        ("Exceptions", info.exceptions),
        ("Examples", examples_block),
    ):
        if body:
            sections.append(f"{heading}:\n{body}")
    click.echo("\n\n".join(sections))


if __name__ == "__main__":
    main()
