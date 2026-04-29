"""Example custom rule: bare SELECT in staging models.

Demonstrates optional structured metadata kwargs (rationale, remediation,
examples) on the @rule decorator. These populate --list-rules output when
provided.
"""

from __future__ import annotations

import re

from dbt_lint.extend import Resource, RuleContext, Violation, rule

# Match WITH as the first SQL keyword, allowing leading whitespace,
# comments, and Jinja blocks before it.
_LEADING_WITH = re.compile(
    r"^(?:\s|{%.*?%}|{#.*?#})*WITH\b",
    re.IGNORECASE | re.DOTALL,
)


@rule(
    id="custom/staging-no-cte-wrapping",
    description="Staging model uses CTE wrapping instead of bare SELECT.",
    rationale=(
        "Staging models use bare SELECT from source() without import CTEs. "
        "This simplifies staging models since they do minimal transformation "
        "(renaming, casting)."
    ),
    remediation=(
        "Remove the CTE and SELECT directly from the source() call. "
        "Move complex logic to an intermediate model."
    ),
    examples=(
        "Violation: WITH src AS (SELECT * FROM {{ source(...) }}) SELECT * FROM src",
        "Pass: SELECT col_a, col_b FROM {{ source('raw', 'users') }}",
    ),
)
def staging_no_cte_wrapping(
    resource: Resource, context: RuleContext
) -> Violation | None:
    if resource.resource_type != "model":
        return None
    if resource.model_type != "staging":
        return None
    if not resource.raw_code:
        return None

    if not _LEADING_WITH.search(resource.raw_code):
        return None

    return context.violation(
        resource,
        f"{resource.resource_name}: staging model uses CTE wrapping;"
        " use bare SELECT from source() per project convention",
    )
