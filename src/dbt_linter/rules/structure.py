"""Structure rules: naming, directories, materializations."""

from __future__ import annotations

import re

from dbt_linter.config import RuleConfig
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import rule

_SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")


@rule(
    id="structure/model-name-format",
    description="Model name is not valid snake_case.",
)
def model_name_format(
    resource: Resource, config: RuleConfig
) -> Violation | None:
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
def model_directories(
    resource: Resource, config: RuleConfig
) -> Violation | None:
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
        message=(
            f"{resource.resource_name}: expected in"
            f" {display}/ directory"
        ),
        severity=config.severity,
        file_path=resource.file_path,
    )


@rule(
    id="structure/source-directories",
    description="Source YAML not in staging directory.",
)
def source_directories(
    resource: Resource, config: RuleConfig
) -> Violation | None:
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
    # This rule checks that tests are colocated with their models.
    # In manifest, test nodes have file_path pointing to the YAML file.
    # We compare directory of test YAML vs directory of the tested model.
    # For now, this is a placeholder that requires manifest parsing context.
    # The actual check will match test resources to their attached models.
    return []


def _check_materialization(
    resource: Resource,
    config: RuleConfig,
    model_type: str,
    rule_id: str,
) -> Violation | None:
    if (
        resource.resource_type != "model"
        or resource.model_type != model_type
    ):
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
def staging_materialization(
    resource: Resource, config: RuleConfig
) -> Violation | None:
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
def marts_materialization(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    return _check_materialization(
        resource, config, "marts", "structure/marts-materialization"
    )
