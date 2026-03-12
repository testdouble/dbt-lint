"""Enforce bare SELECT in staging models (no CTE wrapping).

Project standard 9: staging models use bare SELECT from source() without
import CTEs. Consistent across all 32 sources. Simplifies staging models
since they do minimal transformation.

Detects WITH as the first SQL keyword (ignoring Jinja blocks and whitespace).
"""

from __future__ import annotations

import re

from dbt_linter.extend import Resource, RuleConfig, Violation, rule

# Match WITH as the first SQL keyword, allowing leading whitespace,
# comments, and Jinja blocks before it.
_LEADING_WITH = re.compile(
    r"^(?:\s|{%.*?%}|{#.*?#})*WITH\b",
    re.IGNORECASE | re.DOTALL,
)


@rule(
    id="custom/staging-no-cte-wrapping",
    description="Staging model uses CTE wrapping instead of bare SELECT.",
)
def staging_no_cte_wrapping(resource: Resource, config: RuleConfig) -> Violation | None:
    if resource.resource_type != "model":
        return None
    if resource.model_type != "staging":
        return None
    if not resource.raw_code:
        return None

    if not _LEADING_WITH.search(resource.raw_code):
        return None

    return Violation.from_resource(
        resource,
        f"{resource.resource_name}: staging model uses CTE wrapping;"
        " use bare SELECT from source() per project convention",
    )
