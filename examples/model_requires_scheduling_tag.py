"""Example custom rule: scheduling tag requirement.

Demonstrates optional structured metadata kwargs (rationale, remediation,
examples) on the @rule decorator. These populate --list-rules output when
provided.
"""

from __future__ import annotations

from dbt_lint.extend import Resource, RuleConfig, Violation, rule

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
    rationale=(
        "Tags drive orchestration (scheduled_update, "
        "frequent_update, transformed_hourly, etc.). Every model "
        "should declare which schedule it belongs to."
    ),
    remediation=(
        "Add a scheduling tag to the model's config in dbt_project.yml "
        "or the model's YAML properties."
    ),
    examples=(
        "Violation: fct_orders has tags: [] (no scheduling tag)",
        "Pass: fct_orders has tags: [scheduled_update]",
    ),
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
