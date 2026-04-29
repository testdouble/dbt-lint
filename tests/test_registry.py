"""Tests for the rule registry."""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from dbt_lint.registry import Registry, _synthetic_module_name
from dbt_lint.rules import RuleDef, get_all_rules


def _write_rule_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content))


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


class TestAll:
    def test_no_customs_returns_just_builtins(self):
        subject = Registry()

        all_ids = sorted(r.id for r in subject.all())

        assert all_ids == sorted(r.id for r in subject.builtins())

    def test_returns_builtins_plus_registered_customs(self, tmp_path):
        rule_file = tmp_path / "extra.py"
        _write_rule_file(
            rule_file,
            """\
            from dbt_lint.extend import Resource, RuleContext, Violation, rule

            @rule(id="custom/extra", description="Extra.")
            def extra(resource: Resource, context: RuleContext) -> Violation | None:
                return None
            """,
        )
        subject = Registry()

        subject.register_from_path("extra.py", "custom/extra", tmp_path)

        ids = {r.id for r in subject.all()}
        assert "custom/extra" in ids
        assert ids.issuperset({r.id for r in subject.builtins()})


class TestSyntheticModuleName:
    def test_relative_path(self, tmp_path):
        source = tmp_path / "custom_rules" / "modeling" / "select_distinct.py"

        name = _synthetic_module_name(source, tmp_path)

        assert name == "dbt_lint_custom.custom_rules.modeling.select_distinct"

    def test_flat_file(self, tmp_path):
        source = tmp_path / "my_rule.py"

        name = _synthetic_module_name(source, tmp_path)

        assert name == "dbt_lint_custom.my_rule"


class TestRegisterFromPath:
    def test_loads_per_resource_rule(self, tmp_path):
        _write_rule_file(
            tmp_path / "flag_all.py",
            """\
            from dbt_lint.extend import Resource, RuleContext, Violation, rule

            @rule(id="custom/flag-all", description="Flags everything.")
            def flag_all(resource: Resource, context: RuleContext) -> Violation | None:
                return context.violation(resource, "flagged")
            """,
        )
        subject = Registry()

        subject.register_from_path("flag_all.py", "custom/flag-all", tmp_path)

        customs = [r for r in subject.all() if r.id == "custom/flag-all"]
        assert len(customs) == 1
        assert customs[0].is_per_resource is True

    def test_loads_aggregate_rule(self, tmp_path):
        _write_rule_file(
            tmp_path / "agg.py",
            """\
            from dbt_lint.extend import (
                Resource, Relationship, RuleContext, Violation, rule,
            )

            @rule(id="custom/agg", description="Aggregate.")
            def agg(
                resources: list[Resource],
                relationships: list[Relationship],
                context: RuleContext,
            ) -> list[Violation]:
                return []
            """,
        )
        subject = Registry()

        subject.register_from_path("agg.py", "custom/agg", tmp_path)

        customs = [r for r in subject.all() if r.id == "custom/agg"]
        assert len(customs) == 1
        assert customs[0].is_per_resource is False

    def test_two_rules_in_same_file(self, tmp_path):
        _write_rule_file(
            tmp_path / "multi.py",
            """\
            from dbt_lint.extend import Resource, RuleContext, Violation, rule

            @rule(id="custom/rule-a", description="Rule A.")
            def rule_a(resource: Resource, context: RuleContext) -> Violation | None:
                return None

            @rule(id="custom/rule-b", description="Rule B.")
            def rule_b(resource: Resource, context: RuleContext) -> Violation | None:
                return None
            """,
        )
        sys.modules.pop("dbt_lint_custom.multi", None)
        subject = Registry()

        subject.register_from_path("multi.py", "custom/rule-a", tmp_path)
        subject.register_from_path("multi.py", "custom/rule-b", tmp_path)

        custom_ids = {r.id for r in subject.all() if r.category == "custom"}
        assert custom_ids == {"custom/rule-a", "custom/rule-b"}

    def test_file_not_found(self, tmp_path):
        subject = Registry()

        with pytest.raises(FileNotFoundError, match="file not found"):
            subject.register_from_path("nonexistent.py", "custom/missing", tmp_path)

    def test_no_rule_function(self, tmp_path):
        _write_rule_file(tmp_path / "empty.py", "x = 1\n")
        subject = Registry()

        with pytest.raises(ValueError, match="no @rule function found"):
            subject.register_from_path("empty.py", "custom/empty", tmp_path)

    def test_id_mismatch(self, tmp_path):
        _write_rule_file(
            tmp_path / "wrong_id.py",
            """\
            from dbt_lint.extend import Resource, RuleContext, Violation, rule

            @rule(id="custom/other-name", description="Wrong.")
            def other(resource: Resource, context: RuleContext) -> Violation | None:
                return None
            """,
        )
        subject = Registry()

        with pytest.raises(ValueError, match="no matching"):
            subject.register_from_path("wrong_id.py", "custom/expected-name", tmp_path)

    def test_builtin_id_collision(self, tmp_path):
        _write_rule_file(
            tmp_path / "collision.py",
            """\
            from dbt_lint.extend import Resource, RuleContext, Violation, rule

            @rule(
                id="modeling/too-many-joins",
                description="Collides with built-in.",
            )
            def bad(resource: Resource, context: RuleContext) -> Violation | None:
                return None
            """,
        )
        subject = Registry()

        with pytest.raises(ValueError, match="conflicts with built-in"):
            subject.register_from_path(
                "collision.py", "modeling/too-many-joins", tmp_path
            )

    def test_import_error_wrapped(self, tmp_path):
        _write_rule_file(tmp_path / "bad_syntax.py", "def broken(\n")
        subject = Registry()

        with pytest.raises(ImportError, match="Failed to load custom rule"):
            subject.register_from_path("bad_syntax.py", "custom/bad", tmp_path)

    def test_failed_import_does_not_pollute_sys_modules(self, tmp_path):
        _write_rule_file(
            tmp_path / "poison.py",
            "raise RuntimeError('boom')\n",
        )
        before = {k for k in sys.modules if k.startswith("dbt_lint_custom.")}
        subject = Registry()

        try:
            with pytest.raises(ImportError):
                subject.register_from_path("poison.py", "custom/poison", tmp_path)

            after = {k for k in sys.modules if k.startswith("dbt_lint_custom.")}
            assert after == before
        finally:
            for key in list(sys.modules):
                if key.startswith("dbt_lint_custom.") and key not in before:
                    sys.modules.pop(key, None)

    def test_failed_import_retryable(self, tmp_path):
        _write_rule_file(
            tmp_path / "retry_bad.py",
            "raise RuntimeError('boom')\n",
        )
        before = {k for k in sys.modules if k.startswith("dbt_lint_custom.")}
        subject = Registry()

        try:
            with pytest.raises(ImportError):
                subject.register_from_path("retry_bad.py", "custom/retry", tmp_path)
            with pytest.raises(ImportError):
                subject.register_from_path("retry_bad.py", "custom/retry", tmp_path)
        finally:
            for key in list(sys.modules):
                if key.startswith("dbt_lint_custom.") and key not in before:
                    sys.modules.pop(key, None)

    def test_path_traversal_rejected(self, tmp_path):
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        _write_rule_file(
            tmp_path / "outside" / "evil.py",
            """\
            from dbt_lint.extend import Resource, RuleContext, Violation, rule

            @rule(id="custom/evil", description="Evil.")
            def evil(resource: Resource, context: RuleContext) -> Violation | None:
                return None
            """,
        )
        subject = Registry()

        with pytest.raises(ValueError, match="outside config directory"):
            subject.register_from_path("../outside/evil.py", "custom/evil", project_dir)

    def test_path_traversal_resolving_back_inside(self, tmp_path):
        _write_rule_file(
            tmp_path / "custom_rules" / "valid.py",
            """\
            from dbt_lint.extend import Resource, RuleContext, Violation, rule

            @rule(id="custom/valid", description="Valid.")
            def valid(resource: Resource, context: RuleContext) -> Violation | None:
                return None
            """,
        )
        subject = Registry()

        subject.register_from_path(
            "custom_rules/../custom_rules/valid.py", "custom/valid", tmp_path
        )

        custom_ids = {r.id for r in subject.all() if r.category == "custom"}
        assert custom_ids == {"custom/valid"}

    def test_bad_signature_at_decoration_time(self, tmp_path):
        _write_rule_file(
            tmp_path / "bad_sig.py",
            """\
            from dbt_lint.extend import Resource, rule

            @rule(id="custom/bad-sig", description="Bad.")
            def bad(resource: Resource):
                return None
            """,
        )
        subject = Registry()

        with pytest.raises(ImportError, match="@rule error"):
            subject.register_from_path("bad_sig.py", "custom/bad-sig", tmp_path)
