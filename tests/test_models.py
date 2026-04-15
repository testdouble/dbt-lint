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

    def test_from_resource(self, make_resource):
        resource = make_resource(
            resource_id="model.pkg.stg_orders",
            resource_name="stg_orders",
            file_path="models/staging/stg_orders.sql",
        )
        violation = Violation.from_resource(
            resource, "stg_orders: uses SELECT DISTINCT"
        )
        assert violation.resource_id == "model.pkg.stg_orders"
        assert violation.resource_name == "stg_orders"
        assert violation.file_path == "models/staging/stg_orders.sql"
        assert violation.message == "stg_orders: uses SELECT DISTINCT"
        assert violation.rule_id == ""
        assert violation.severity == ""
        assert violation.patch_path == ""

    def test_from_resource_carries_patch_path(self, make_resource):
        resource = make_resource(
            patch_path="project://models/staging/_staging.yml",
        )
        violation = Violation.from_resource(resource, "test")
        assert violation.patch_path == "models/staging/_staging.yml"

    def test_from_resource_strips_patch_prefix(self, make_resource):
        resource = make_resource(patch_path="my_project://models/foo.yml")
        violation = Violation.from_resource(resource, "test")
        assert violation.patch_path == "models/foo.yml"

    def test_from_resource_no_prefix_passthrough(self, make_resource):
        resource = make_resource(patch_path="models/staging/_staging.yml")
        violation = Violation.from_resource(resource, "test")
        assert violation.patch_path == "models/staging/_staging.yml"


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
