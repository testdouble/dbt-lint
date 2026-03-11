"""Tests for governance rules."""

from dbt_linter.rules.governance import (
    exposures_depend_on_private_models,
    public_models_without_contract,
    undocumented_public_models,
)


class TestPublicModelsWithoutContract:
    def test_flags_public_without_contract(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            is_public=True,
            is_contract_enforced=False,
        )
        v = public_models_without_contract(r, default_config)
        assert v is not None

    def test_clean_public_with_contract(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            is_public=True,
            is_contract_enforced=True,
        )
        assert public_models_without_contract(r, default_config) is None

    def test_clean_private_model(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            is_public=False,
            is_contract_enforced=False,
        )
        assert public_models_without_contract(r, default_config) is None


class TestUndocumentedPublicModels:
    def test_flags_public_missing_description(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            is_public=True,
            is_described=False,
        )
        v = undocumented_public_models(r, default_config)
        assert v is not None
        assert "missing description" in v.message

    def test_flags_public_missing_column_docs(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            is_public=True,
            is_described=True,
            number_of_columns=5,
            number_of_documented_columns=3,
        )
        v = undocumented_public_models(r, default_config)
        assert v is not None
        assert "3/5" in v.message

    def test_clean_fully_documented(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            is_public=True,
            is_described=True,
            number_of_columns=5,
            number_of_documented_columns=5,
        )
        assert undocumented_public_models(r, default_config) is None


class TestExposuresDependOnPrivateModels:
    def test_flags_exposure_with_private_parent(
        self, make_resource, make_relationship, default_config
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
        vs = exposures_depend_on_private_models(
            [exposure, parent], rels, default_config
        )
        assert len(vs) == 1
        assert "fct_orders" in vs[0].message

    def test_clean_exposure_with_public_parent(
        self, make_resource, make_relationship, default_config
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
        vs = exposures_depend_on_private_models(
            [exposure, parent], rels, default_config
        )
        assert len(vs) == 0
