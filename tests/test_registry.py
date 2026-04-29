"""Tests for the rule registry."""

from dbt_lint.registry import Registry
from dbt_lint.rules import RuleDef, get_all_rules


class TestBuiltins:
    def test_returns_rule_defs(self):
        subject = Registry()

        rules = subject.builtins()

        assert all(isinstance(r, RuleDef) for r in rules)

    def test_ids_are_unique(self):
        subject = Registry()

        ids = [r.id for r in subject.builtins()]

        assert len(ids) == len(set(ids))

    def test_parity_with_get_all_rules(self):
        subject = Registry()

        registry_ids = sorted(r.id for r in subject.builtins())

        assert registry_ids == sorted(r.id for r in get_all_rules())
