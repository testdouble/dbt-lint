"""Tests for governance rules."""

from dbt_lint.rules.governance import (
    exposures_depend_on_private_models,
    intermediate_public_access,
    public_models_without_contract,
    undocumented_public_models,
)


class TestPublicModelsWithoutContract:
    def test_flags_public_without_contract(self, make_resource, default_context):
        resource = make_resource(
            resource_type="model",
            is_public=True,
            is_contract_enforced=False,
        )

        violation = public_models_without_contract(resource, default_context)

        assert violation is not None
        assert "without contract enforcement" in violation.message

    def test_clean_public_with_contract(self, make_resource, default_context):
        resource = make_resource(
            resource_type="model",
            is_public=True,
            is_contract_enforced=True,
        )

        assert public_models_without_contract(resource, default_context) is None

    def test_clean_private_model(self, make_resource, default_context):
        resource = make_resource(
            resource_type="model",
            is_public=False,
            is_contract_enforced=False,
        )

        assert public_models_without_contract(resource, default_context) is None


class TestUndocumentedPublicModels:
    def test_flags_public_missing_description(self, make_resource, default_context):
        resource = make_resource(
            resource_type="model",
            is_public=True,
            is_described=False,
        )

        violation = undocumented_public_models(resource, default_context)

        assert violation is not None
        assert "missing description" in violation.message

    def test_flags_public_missing_column_docs(self, make_resource, default_context):
        resource = make_resource(
            resource_type="model",
            is_public=True,
            is_described=True,
            number_of_columns=5,
            number_of_documented_columns=3,
        )

        violation = undocumented_public_models(resource, default_context)

        assert violation is not None
        assert "3/5 columns documented" in violation.message

    def test_clean_fully_documented(self, make_resource, default_context):
        resource = make_resource(
            resource_type="model",
            is_public=True,
            is_described=True,
            number_of_columns=5,
            number_of_documented_columns=5,
        )

        assert undocumented_public_models(resource, default_context) is None


class TestIntermediatePublicAccess:
    def test_flags_public_intermediate(self, make_resource, default_context):
        resource = make_resource(
            resource_type="model",
            model_type="intermediate",
            is_public=True,
        )

        violation = intermediate_public_access(resource, default_context)

        assert violation is not None
        assert "intermediate" in violation.message
        assert "public" in violation.message

    def test_clean_private_intermediate(self, make_resource, default_context):
        resource = make_resource(
            resource_type="model",
            model_type="intermediate",
            is_public=False,
        )

        assert intermediate_public_access(resource, default_context) is None

    def test_clean_public_mart(self, make_resource, default_context):
        resource = make_resource(
            resource_type="model",
            model_type="marts",
            is_public=True,
        )

        assert intermediate_public_access(resource, default_context) is None

    def test_skips_non_model(self, make_resource, default_context):
        resource = make_resource(
            resource_type="source",
            model_type="intermediate",
            is_public=True,
        )

        assert intermediate_public_access(resource, default_context) is None


class TestExposuresDependOnPrivateModels:
    def test_flags_exposure_with_private_parent(
        self, make_resource, make_relationship, default_context
    ):
        exposure = make_resource(
            resource_id="exposure.pkg.dashboard",
            resource_type="exposure",
            resource_name="dashboard",
            model_type="",
        )
        parent = make_resource(
            resource_id="model.pkg.fct_orders",
            resource_type="model",
            resource_name="fct_orders",
            is_public=False,
        )
        rels = [
            make_relationship(
                parent="model.pkg.fct_orders",
                child="exposure.pkg.dashboard",
                parent_resource_type="model",
                child_resource_type="exposure",
            ),
        ]

        violations = exposures_depend_on_private_models(
            [exposure, parent], rels, default_context
        )

        assert len(violations) == 1
        assert "fct_orders" in violations[0].message

    def test_clean_exposure_with_public_parent(
        self, make_resource, make_relationship, default_context
    ):
        exposure = make_resource(
            resource_id="exposure.pkg.dashboard",
            resource_type="exposure",
            model_type="",
        )
        parent = make_resource(
            resource_id="model.pkg.fct_orders",
            resource_type="model",
            is_public=True,
        )
        rels = [
            make_relationship(
                parent="model.pkg.fct_orders",
                child="exposure.pkg.dashboard",
                parent_resource_type="model",
                child_resource_type="exposure",
            ),
        ]

        violations = exposures_depend_on_private_models(
            [exposure, parent], rels, default_context
        )

        assert len(violations) == 0
