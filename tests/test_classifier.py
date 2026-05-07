"""Tests for classifier.py: model-type classification heuristics."""

from __future__ import annotations

from dbt_lint.classifier import classify_model_type
from dbt_lint.config import DEFAULTS


class TestPrefixMatch:
    """Pass 1: prefix matches against configured lists."""

    def test_staging_prefix(self):
        assert (
            classify_model_type(
                "stg_orders", "models/anywhere/stg_orders.sql", DEFAULTS
            )
            == "staging"
        )

    def test_intermediate_prefix(self):
        assert (
            classify_model_type(
                "int_orders", "models/anywhere/int_orders.sql", DEFAULTS
            )
            == "intermediate"
        )

    def test_base_prefix(self):
        assert (
            classify_model_type(
                "base_orders", "models/anywhere/base_orders.sql", DEFAULTS
            )
            == "base"
        )

    def test_other_prefix(self):
        assert (
            classify_model_type("rpt_sales", "models/anywhere/rpt_sales.sql", DEFAULTS)
            == "other"
        )

    def test_multiple_configured_prefixes(self):
        """A type can have multiple prefixes; any of them matches."""
        params = {**DEFAULTS, "staging_prefixes": ["stg_", "staging_"]}
        assert (
            classify_model_type("staging_orders", "models/x.sql", params) == "staging"
        )


class TestFolderMatch:
    """Pass 2: directory-based fallback when no prefix matches."""

    def test_staging_folder(self):
        assert (
            classify_model_type("orders", "models/staging/orders.sql", DEFAULTS)
            == "staging"
        )

    def test_intermediate_folder(self):
        assert (
            classify_model_type("orders", "models/intermediate/orders.sql", DEFAULTS)
            == "intermediate"
        )

    def test_marts_folder(self):
        assert (
            classify_model_type("fct_orders", "models/marts/fct_orders.sql", DEFAULTS)
            == "marts"
        )

    def test_base_folder(self):
        assert (
            classify_model_type("orders", "models/base/orders.sql", DEFAULTS) == "base"
        )

    def test_nested_folder_match(self):
        assert (
            classify_model_type(
                "orders", "models/staging/stripe/v2/orders.sql", DEFAULTS
            )
            == "staging"
        )

    def test_folder_name_must_be_a_full_path_segment(self):
        """A substring of a path segment does not match — segments are split on /."""
        assert (
            classify_model_type("orders", "models/staging-prep/orders.sql", DEFAULTS)
            == "other"
        )


class TestPrefixTakesPrecedence:
    """When both passes would match, the prefix pass wins."""

    def test_prefix_wins_over_folder(self):
        assert (
            classify_model_type("stg_orders", "models/marts/stg_orders.sql", DEFAULTS)
            == "staging"
        )


class TestOtherFallback:
    """No prefix and no folder match falls through to 'other'."""

    def test_no_match_returns_other(self):
        assert (
            classify_model_type("custom", "models/custom/custom.sql", DEFAULTS)
            == "other"
        )


class TestEmptyConfigKeys:
    """Edge cases when the configured prefix or folder values are empty."""

    def test_empty_prefix_list_falls_through_to_folder(self):
        """marts_prefixes defaults to []; classification still works via folder."""
        assert DEFAULTS["marts_prefixes"] == []
        assert (
            classify_model_type("fct_orders", "models/marts/fct_orders.sql", DEFAULTS)
            == "marts"
        )

    def test_all_prefixes_empty(self):
        """When every prefix list is empty, only folder matching can classify."""
        params = {
            **DEFAULTS,
            "staging_prefixes": [],
            "intermediate_prefixes": [],
            "marts_prefixes": [],
            "base_prefixes": [],
            "other_prefixes": [],
        }
        assert (
            classify_model_type("stg_orders", "models/staging/x.sql", params)
            == "staging"
        )
        assert (
            classify_model_type("stg_orders", "models/elsewhere/x.sql", params)
            == "other"
        )
