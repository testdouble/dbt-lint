"""Tests for Violation dataclass logic."""

from dbt_lint.models import Violation


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
