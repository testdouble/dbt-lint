"""Tests for Violation dataclass logic."""

import pytest

from dbt_lint.models import Violation, strip_patch_prefix


class TestViolation:
    def test_construction(self):
        violation = Violation(
            rule_id="modeling/root-models",
            resource_id="model.pkg.orphan",
            resource_name="orphan",
            message="orphan: model has no parents",
            severity="warn",
            file_path="models/orphan.sql",
        )
        assert violation.rule_id == "modeling/root-models"
        assert violation.severity == "warn"
        assert violation.file_path == "models/orphan.sql"
        assert violation.patch_path == ""


class TestStripPatchPrefix:
    @pytest.mark.parametrize(
        ("input_path", "expected"),
        [
            ("project://models/staging/_staging.yml", "models/staging/_staging.yml"),
            ("my_project://models/foo.yml", "models/foo.yml"),
            ("models/staging/_staging.yml", "models/staging/_staging.yml"),
            ("", ""),
            (None, ""),
        ],
    )
    def test_strip_patch_prefix(self, input_path, expected):
        assert strip_patch_prefix(input_path) == expected
