"""Shared fixtures for rule tests."""

import pytest

from dbt_lint.config import DEFAULTS
from dbt_lint.rules import RuleContext


@pytest.fixture
def default_context():
    """Default RuleContext with all params, severity=warn."""
    return RuleContext(
        params={**DEFAULTS},
        _rule_id="test/rule",
        _severity="warn",
    )
