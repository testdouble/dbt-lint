"""Require models to have at least one scheduling tag.

Project standard 7: tags drive orchestration (scheduled_update,
frequent_update, transformed_hourly, etc.). Every model should
declare which schedule it belongs to.
"""

from __future__ import annotations

from dbt_linter.extend import Resource, RuleConfig, Violation, rule

_SCHEDULING_TAGS = (
    "scheduled_update",
    "frequent_update",
)

_SCHEDULING_PREFIXES = (
    "weekly_",
    "transformed_",
)


@rule(
    id="custom/model-requires-scheduling-tag",
    description="Model missing a scheduling tag.",
)
def model_requires_scheduling_tag(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    if resource.resource_type != "model":
        return None

    for tag in resource.tags:
        if tag in _SCHEDULING_TAGS:
            return None
        if any(tag.startswith(prefix) for prefix in _SCHEDULING_PREFIXES):
            return None

    valid = list(_SCHEDULING_TAGS) + [p + "*" for p in _SCHEDULING_PREFIXES]
    return Violation.from_resource(
        resource,
        f"{resource.resource_name}: missing a scheduling tag"
        f" (expected one of: {', '.join(valid)})",
    )
