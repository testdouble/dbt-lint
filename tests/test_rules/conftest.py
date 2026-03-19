"""Shared fixtures for rule tests."""

import pytest

from dbt_linter.config import DEFAULTS, RuleConfig


@pytest.fixture
def default_config():
    """Default RuleConfig with all params."""
    return RuleConfig(
        enabled=True,
        severity="warn",
        exclude_resources=[],
        params={**DEFAULTS},
    )
