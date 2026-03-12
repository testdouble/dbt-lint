"""Flag models using SELECT DISTINCT (prefer GROUP BY or QUALIFY).

Deviation report pattern 5: litwak's reviews consistently flag SELECT DISTINCT
as a code smell. GROUP BY is more explicit about intent, and QUALIFY with
ROW_NUMBER() handles deduplication more precisely.

Ignores COUNT(DISTINCT ...) which is a legitimate aggregate.
"""

from __future__ import annotations

import re

from dbt_linter.extend import Resource, RuleConfig, Violation, rule

# Match SELECT DISTINCT but not COUNT(DISTINCT or similar function(DISTINCT.
# Handles whitespace/newlines between SELECT and DISTINCT.
_SELECT_DISTINCT = re.compile(r"\bSELECT\s+DISTINCT\b", re.IGNORECASE | re.DOTALL)
_FUNC_DISTINCT = re.compile(r"\w+\s*\(\s*DISTINCT\b", re.IGNORECASE)


@rule(
    id="custom/avoid-select-distinct",
    description="Model uses SELECT DISTINCT instead of GROUP BY.",
)
def avoid_select_distinct(resource: Resource, config: RuleConfig) -> Violation | None:
    if resource.resource_type != "model":
        return None
    if not resource.raw_code:
        return None

    # Check for SELECT DISTINCT, excluding function(DISTINCT ...) patterns.
    # Strategy: find all DISTINCT usages, subtract those inside functions.
    select_matches = _SELECT_DISTINCT.findall(resource.raw_code)
    if not select_matches:
        return None

    # If every DISTINCT is inside a function call, it's fine.
    func_count = len(_FUNC_DISTINCT.findall(resource.raw_code))
    select_count = len(select_matches)
    if select_count <= func_count:
        return None

    return Violation.from_resource(
        resource,
        f"{resource.resource_name}: uses SELECT DISTINCT;"
        " prefer GROUP BY or QUALIFY for deduplication",
    )
