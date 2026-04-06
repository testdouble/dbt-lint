"""Integration tests: rule registry discovery and rules index generation."""

from dbt_lint.rules import RuleInfo, generate_rules_index, get_all_rules


class TestGetAllRules:
    def test_discovers_rules_across_all_categories(self):
        rules = get_all_rules()
        by_category = {r.category for r in rules}
        expected_categories = {
            "modeling",
            "testing",
            "documentation",
            "structure",
            "performance",
            "governance",
        }
        assert by_category == expected_categories
        assert len(rules) >= len(expected_categories)

    def test_all_rules_have_unique_ids(self):
        rules = get_all_rules()
        ids = [r.id for r in rules]
        assert len(ids) == len(set(ids))


class TestAllRulesHaveRationale:
    def test_all_rules_have_rationale(self):
        index = generate_rules_index()
        for info in index:
            assert info.rationale, f"{info.id} is missing rationale"

    def test_all_rules_have_remediation(self):
        index = generate_rules_index()
        for info in index:
            assert info.remediation, f"{info.id} is missing remediation"


class TestGenerateRulesIndex:
    def test_returns_entry_for_every_rule(self):
        index = generate_rules_index()
        assert len(index) == len(get_all_rules())

    def test_sorted_by_id(self):
        index = generate_rules_index()
        ids = [r.id for r in index]
        assert ids == sorted(ids)

    def test_all_have_description_and_category(self):
        index = generate_rules_index()
        for info in index:
            assert info.description, f"{info.id} missing description"
            assert info.category, f"{info.id} missing category"

    def test_categories_match_id_prefix(self):
        index = generate_rules_index()
        for info in index:
            assert info.id.startswith(info.category + "/")

    def test_returns_rule_info_instances(self):
        index = generate_rules_index()
        for info in index:
            assert isinstance(info, RuleInfo)

    def test_is_per_resource_is_bool(self):
        index = generate_rules_index()
        for info in index:
            assert isinstance(info.is_per_resource, bool)

    def test_remediation_and_exceptions_are_strings(self):
        index = generate_rules_index()
        for info in index:
            assert isinstance(info.remediation, str)
            assert isinstance(info.exceptions, str)

    def test_examples_are_tuples(self):
        index = generate_rules_index()
        for info in index:
            assert isinstance(info.examples, tuple), f"{info.id} examples not a tuple"
