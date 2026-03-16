"""Structure rules: naming, directories, materializations, column conventions."""

from __future__ import annotations

import re
from typing import Any

from dbt_linter.config import RuleConfig
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import rule

_SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")


@rule(
    id="structure/model-name-format",
    description="Model name is not valid snake_case.",
)
def model_name_format(resource: Resource, config: RuleConfig) -> Violation | None:
    """Model names should be valid snake_case.

    Consistent naming prevents quoting issues across warehouses and
    makes models predictable to reference in SQL and Jinja. Allows
    lowercase letters, numbers, and underscores.
    """
    if resource.resource_type != "model":
        return None
    if _SNAKE_CASE.match(resource.resource_name):
        return None
    return Violation(
        rule_id="structure/model-name-format",
        resource_id=resource.resource_id,
        resource_name=resource.resource_name,
        message=(
            f"{resource.resource_name}: model name must be"
            " snake_case (lowercase letters, numbers, underscores)"
        ),
        severity=config.severity,
        file_path=resource.file_path,
    )


@rule(
    id="structure/model-naming-conventions",
    description="Model name doesn't match prefix for its model type.",
)
def model_naming_conventions(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    """Model names should start with the expected prefix for their model type.

    Prefixes (stg_, int_, fct_, dim_) make a model's layer and purpose
    immediately obvious from its name. They also enable directory-based
    selectors like `dbt run --select staging.*`.

    Configurable via <model_type>_prefixes (e.g., staging_prefixes:
    ["stg_"]).

    Examples:
        Violation: users (marts model, no fct_ or dim_ prefix)
        Pass: fct_orders, dim_customers, stg_users
    """
    if resource.resource_type != "model" or not resource.model_type:
        return None
    prefixes_key = f"{resource.model_type}_prefixes"
    prefixes = config.params.get(prefixes_key, [])
    if not prefixes:
        return None
    if not any(resource.resource_name.startswith(p) for p in prefixes):
        return Violation(
            rule_id="structure/model-naming-conventions",
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=(
                f"{resource.resource_name}: {resource.model_type} model"
                f" should start with one of {prefixes}"
            ),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="structure/model-directories",
    description="Model not in expected directory for its model type.",
)
def model_directories(resource: Resource, config: RuleConfig) -> Violation | None:
    """Models should live in the directory matching their model type.

    Directory structure should mirror the DAG layers (staging,
    intermediate, marts). A model in the wrong directory creates
    confusion about its role and breaks directory-based selectors.

    Configurable via <model_type>_folder_name (e.g.,
    staging_folder_name: "staging").
    """
    if resource.resource_type != "model" or not resource.model_type:
        return None
    folder_key = f"{resource.model_type}_folder_name"
    expected = config.params.get(folder_key)
    if not expected:
        return None
    folders = expected if isinstance(expected, list) else [expected]
    path = f"/{resource.file_path}"
    if any(f"/{f}/" in path for f in folders):
        return None
    display = folders[0] if len(folders) == 1 else f"one of {folders}"
    return Violation(
        rule_id="structure/model-directories",
        resource_id=resource.resource_id,
        resource_name=resource.resource_name,
        message=(f"{resource.resource_name}: expected in {display}/ directory"),
        severity=config.severity,
        file_path=resource.file_path,
    )


@rule(
    id="structure/source-directories",
    description="Source YAML not in staging directory.",
)
def source_directories(resource: Resource, config: RuleConfig) -> Violation | None:
    """Source YAML definitions should live in the staging directory.

    Sources feed staging models, so colocating source YAML with the
    staging directory keeps the source-to-staging relationship visible
    in the file tree.

    Configurable via staging_folder_name (default: "staging").
    """
    if resource.resource_type != "source":
        return None
    staging_folder = config.params.get("staging_folder_name", "staging")
    if f"/{staging_folder}/" not in f"/{resource.file_path}":
        return Violation(
            rule_id="structure/source-directories",
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=(
                f"{resource.resource_name}: source YAML expected in"
                f" {staging_folder}/ directory"
            ),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="structure/test-directories",
    description="Test YAML in different directory than model.",
)
def test_directories(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    """Test YAML should be colocated with the model it tests.

    Placeholder rule, not yet implemented.
    """
    # Unimplemented: checks test YAML colocation with tested models.
    return []


@rule(
    id="structure/staging-naming-convention",
    description="Staging model name doesn't follow stg_<source>__<entity> pattern.",
)
def staging_naming_convention(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    """Staging models should follow the stg_<source>__<entity> pattern.

    The double underscore separates the source system from the entity
    name, making it unambiguous which upstream source a staging model
    wraps. This is the dbt community convention from the dbt style
    guide.

    Configurable via staging_prefixes (list of accepted prefixes).

    Examples:
        Violation: stg_users (missing __ separator)
        Pass: stg_stripe__payments, stg_shopify__orders
    """
    if resource.resource_type != "model" or resource.model_type != "staging":
        return None
    prefixes = config.params.get("staging_prefixes", [])
    matched_prefix = None
    for p in prefixes:
        if resource.resource_name.startswith(p):
            matched_prefix = p
            break
    if matched_prefix is None:
        return None
    remainder = resource.resource_name[len(matched_prefix) :]
    if "__" in remainder:
        return None
    return Violation(
        rule_id="structure/staging-naming-convention",
        resource_id=resource.resource_id,
        resource_name=resource.resource_name,
        message=(
            f"{resource.resource_name}: staging model should follow"
            " stg_<source>__<entity> pattern (missing __ separator)"
        ),
        severity=config.severity,
        file_path=resource.file_path,
    )


def _check_materialization(
    resource: Resource,
    config: RuleConfig,
    model_type: str,
    rule_id: str,
) -> Violation | None:
    if resource.resource_type != "model" or resource.model_type != model_type:
        return None
    allowed_key = f"{model_type}_allowed_materializations"
    allowed = config.params.get(allowed_key, [])
    if not allowed:
        return None
    if resource.materialization not in allowed:
        return Violation(
            rule_id=rule_id,
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=(
                f"{resource.resource_name}:"
                f" {resource.materialization} not allowed"
                f" for {model_type} (allowed: {allowed})"
            ),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="structure/staging-materialization",
    description="Staging model not in allowed materializations.",
)
def staging_materialization(resource: Resource, config: RuleConfig) -> Violation | None:
    """Staging models should use allowed materializations (typically view).

    Staging models are lightweight transformations (renaming, casting)
    of source data. Views avoid redundant storage. Some teams allow
    table or incremental for high-volume sources.

    Configurable via staging_allowed_materializations.
    """
    return _check_materialization(
        resource, config, "staging", "structure/staging-materialization"
    )


@rule(
    id="structure/intermediate-materialization",
    description="Intermediate model not in allowed materializations.",
)
def intermediate_materialization(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    """Intermediate models should use allowed materializations.

    Intermediates sit between staging and marts. Ephemeral or view is
    common; table if the intermediate is reused by many downstream
    models.

    Configurable via intermediate_allowed_materializations.
    """
    return _check_materialization(
        resource,
        config,
        "intermediate",
        "structure/intermediate-materialization",
    )


@rule(
    id="structure/marts-materialization",
    description="Marts model not in allowed materializations.",
)
def marts_materialization(resource: Resource, config: RuleConfig) -> Violation | None:
    """Marts models should use allowed materializations (typically table).

    Marts are the consumption layer queried by BI tools and analysts.
    Tables provide stable query performance; incremental for large
    fact tables.

    Configurable via marts_allowed_materializations.
    """
    return _check_materialization(
        resource, config, "marts", "structure/marts-materialization"
    )


_YAML_NAMING_RE = re.compile(
    r"_[a-z][a-z0-9_]*__(?:models|sources|docs)\.(?:yml|yaml|md)$"
)


@rule(
    id="structure/yaml-file-naming",
    description="YAML/doc file doesn't follow _<dir>__<type> naming convention.",
)
def yaml_file_naming(resource: Resource, config: RuleConfig) -> Violation | None:
    """YAML property files should follow the _<directory>__<type>.yml convention.

    A consistent naming pattern (_staging__models.yml,
    _staging__sources.yml) makes property files discoverable and
    groups them visually at the top of directory listings due to the
    leading underscore.

    Examples:
        Violation: schema.yml, sources.yml
        Pass: _staging__models.yml, _marts__sources.yml
    """
    if resource.resource_type == "source":
        yaml_path = resource.file_path
    elif resource.resource_type == "model" and resource.patch_path:
        # patch_path format: "project://path/to/file.yml"
        yaml_path = resource.patch_path.split("://", 1)[-1]
    else:
        return None

    filename = yaml_path.rsplit("/", 1)[-1]
    if not filename.endswith((".yml", ".yaml")):
        return None
    if _YAML_NAMING_RE.search(filename):
        return None
    return Violation(
        rule_id="structure/yaml-file-naming",
        resource_id=resource.resource_id,
        resource_name=resource.resource_name,
        message=(
            f"{resource.resource_name}: YAML file '{filename}' should follow"
            " _<directory>__<type>.yml naming convention"
        ),
        severity=config.severity,
        file_path=yaml_path,
    )


def _check_column_naming(
    resource: Resource,
    conventions: dict[str, Any],
) -> list[str]:
    """Check a resource's columns against naming convention config.

    Returns a list of violation messages (empty if all columns pass).
    """
    messages: list[str] = []
    forbidden: dict[str, str] = conventions.get("forbidden_suffixes", {})
    bool_prefixes: list[str] = conventions.get("boolean_prefixes", [])
    type_suffixes: dict[str, str] = conventions.get("type_suffixes", {})

    for col in resource.columns:
        name = col.name.lower()

        for suffix, suggestion in forbidden.items():
            if name.endswith(suffix):
                messages.append(
                    f"{resource.resource_name}.{col.name}:"
                    f" suffix '{suffix}' should be"
                    f" '{suggestion}'"
                )

        if (
            bool_prefixes
            and col.data_type.lower() == "boolean"
            and not any(name.startswith(p) for p in bool_prefixes)
        ):
            messages.append(
                f"{resource.resource_name}.{col.name}:"
                f" boolean column should start with"
                f" one of {bool_prefixes}"
            )

        if type_suffixes and col.data_type:
            dt = col.data_type.lower()
            if dt in type_suffixes:
                expected = type_suffixes[dt]
                if not name.endswith(expected):
                    messages.append(
                        f"{resource.resource_name}.{col.name}:"
                        f" {dt} column should end with"
                        f" '{expected}'"
                    )

    return messages


@rule(
    id="structure/column-naming-conventions",
    description="Column name violates naming conventions.",
)
def column_naming_conventions(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    """Column names should follow configured naming conventions.

    Checks forbidden suffixes (e.g., _date instead of _at), boolean
    prefixes (is_, has_), and type-based suffixes. Disabled by default
    (no conventions configured).

    Configurable via column_naming_conventions with sub-keys:
    forbidden_suffixes, boolean_prefixes, type_suffixes.
    """
    conventions = config.params.get("column_naming_conventions")
    if not conventions:
        return []

    violations: list[Violation] = []
    for resource in resources:
        if resource.resource_type != "model":
            continue
        for msg in _check_column_naming(resource, conventions):
            violations.append(Violation.from_resource(resource, msg))
    return violations
