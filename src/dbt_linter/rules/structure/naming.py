"""Naming and directory rules: model names, prefixes, directories, YAML conventions."""

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
    rationale=(
        "Model names should be valid snake_case."
        "\n\n"
        "Consistent naming prevents quoting issues across warehouses and "
        "makes models predictable to reference in SQL and Jinja. Allows "
        "lowercase letters, numbers, and underscores."
    ),
    remediation=(
        "Rename the model file and all ref() calls to use "
        "snake_case. Avoid dots (conflict with database.schema.object "
        "notation) and abbreviations."
    ),
)
def model_name_format(resource: Resource, config: RuleConfig) -> Violation | None:
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
    rationale=(
        "Model names should start with the expected prefix for their model "
        "type."
        "\n\n"
        "Prefixes (stg_, int_, fct_, dim_) make a model's layer and purpose "
        "immediately obvious from its name. They also enable directory-based "
        "selectors like `dbt run --select staging.*`."
        "\n\n"
        "Configurable via <model_type>_prefixes (e.g., staging_prefixes: "
        '["stg_"]).'
    ),
    remediation=(
        "Rename the model with the appropriate prefix for its layer. "
        "Update all ref() calls across the project."
    ),
    exceptions=(
        "Utility models (e.g., date spines, mappings) that don't "
        "belong to a standard layer."
    ),
    examples=(
        "Violation: users (marts model, no fct_ or dim_ prefix)",
        "Pass: fct_orders, dim_customers, stg_users",
    ),
)
def model_naming_conventions(
    resource: Resource, config: RuleConfig
) -> Violation | None:
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
    rationale=(
        "Models should live in the directory matching their model type."
        "\n\n"
        "Directory structure should mirror the DAG layers (staging, "
        "intermediate, marts). A model in the wrong directory creates "
        "confusion about its role and breaks directory-based selectors."
        "\n\n"
        "Configurable via <model_type>_folder_name (e.g., "
        'staging_folder_name: "staging").'
    ),
    remediation=(
        "Move the model file to the correct directory for its model "
        "type. For staging models, nest in staging/<source_name>/."
    ),
)
def model_directories(resource: Resource, config: RuleConfig) -> Violation | None:
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
    rationale=(
        "Source YAML definitions should live in the staging directory."
        "\n\n"
        "Sources feed staging models, so colocating source YAML with the "
        "staging directory keeps the source-to-staging relationship visible "
        "in the file tree."
        "\n\n"
        'Configurable via staging_folder_name (default: "staging").'
    ),
    remediation=(
        "Move the source YAML file to staging/<source_name>/ "
        "alongside the staging models that consume it."
    ),
)
def source_directories(resource: Resource, config: RuleConfig) -> Violation | None:
    if resource.resource_type != "source":
        return None
    staging_folder = config.params.get("staging_folder_name", "staging")
    path = f"/{resource.file_path}"
    if f"/{staging_folder}/" not in path:
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
    rationale=(
        "Test YAML should be colocated with the model it tests."
        "\n\n"
        "Each subdirectory should contain one YAML file with tests and "
        "docs for all models in that directory. When YAML lives elsewhere, "
        "the relationship between model and its tests is invisible in the "
        "file tree."
    ),
    remediation=("Move test YAML into the same directory as the model(s) it tests."),
)
def check_yaml_colocation(resource: Resource, config: RuleConfig) -> Violation | None:
    if resource.resource_type != "model" or not resource.patch_path:
        return None
    yaml_path = resource.patch_path.split("://", 1)[-1]
    model_dir = resource.file_path.rsplit("/", 1)[0]
    yaml_dir = yaml_path.rsplit("/", 1)[0]
    if model_dir == yaml_dir:
        return None
    return Violation(
        rule_id="structure/test-directories",
        resource_id=resource.resource_id,
        resource_name=resource.resource_name,
        message=(
            f"{resource.resource_name}: YAML in {yaml_dir}/"
            f" but model is in {model_dir}/"
        ),
        severity=config.severity,
        file_path=resource.file_path,
    )


@rule(
    id="structure/staging-naming-convention",
    description="Staging model name doesn't follow stg_<source>__<entity> pattern.",
    rationale=(
        "Staging models should follow the stg_<source>__<entity> pattern."
        "\n\n"
        "The double underscore separates the source system from the entity "
        "name, making it unambiguous which upstream source a staging model "
        "wraps. This is the dbt community convention from the dbt style "
        "guide."
        "\n\n"
        "Configurable via staging_prefixes (list of accepted prefixes)."
    ),
    remediation=(
        "Rename the model to stg_<source>__<entity>s.sql with a "
        "double underscore separating source from entity. Update "
        "all ref() calls."
    ),
    examples=(
        "Violation: stg_users (missing __ separator)",
        "Pass: stg_stripe__payments, stg_shopify__orders",
    ),
)
def staging_naming_convention(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    if resource.resource_type != "model" or resource.model_type != "staging":
        return None
    prefixes = config.params.get("staging_prefixes", [])
    matched_prefix = next(
        (p for p in prefixes if resource.resource_name.startswith(p)), None
    )
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


_YAML_NAMING_RE = re.compile(
    r"_[a-z][a-z0-9_]*__(?:models|sources|docs)\.(?:yml|yaml|md)$"
)


@rule(
    id="structure/yaml-file-naming",
    description="YAML/doc file doesn't follow _<dir>__<type> naming convention.",
    rationale=(
        "YAML property files should follow the _<directory>__<type>.yml "
        "convention."
        "\n\n"
        "A consistent naming pattern (_staging__models.yml, "
        "_staging__sources.yml) makes property files discoverable and "
        "groups them visually at the top of directory listings due to the "
        "leading underscore. Including the directory name speeds up fuzzy "
        "file search."
    ),
    remediation=(
        "Rename the YAML file to _<directory>__<type>.yml where "
        "type is models, sources, or docs. Use _<dir>__docs.md for "
        "doc blocks."
    ),
    examples=(
        "Violation: schema.yml, sources.yml",
        "Pass: _staging__models.yml, _marts__sources.yml",
    ),
)
def yaml_file_naming(resource: Resource, config: RuleConfig) -> Violation | None:
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
            data_type = col.data_type.lower()
            if data_type in type_suffixes:
                expected = type_suffixes[data_type]
                if not name.endswith(expected):
                    messages.append(
                        f"{resource.resource_name}.{col.name}:"
                        f" {data_type} column should end with"
                        f" '{expected}'"
                    )

    return messages


@rule(
    id="structure/column-naming-conventions",
    description="Column name violates naming conventions.",
    rationale=(
        "Column names should follow configured naming conventions."
        "\n\n"
        "Checks forbidden suffixes (e.g., _date instead of _at), boolean "
        "prefixes (is_, has_), and type-based suffixes. Disabled by default "
        "(no conventions configured)."
        "\n\n"
        "Configurable via column_naming_conventions with sub-keys: "
        "forbidden_suffixes, boolean_prefixes, type_suffixes."
    ),
    remediation=(
        "Rename columns in the staging model where they are first "
        "introduced. Update downstream references accordingly."
    ),
    exceptions=(
        "Columns inherited from external APIs or regulatory schemas "
        "where renaming would break compliance or integration."
    ),
)
def column_naming_conventions(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
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
