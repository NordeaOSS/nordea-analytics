from datetime import datetime
from typing import Any, Union

import pytest

from nordea_analytics import (
    AmortisationType,
    AssetType,
    CapitalCentres,
    CapitalCentreTypes,
    Issuers,
    NordeaAnalyticsService,
)


class TestSearchBonds:
    """Test class for searching for bonds."""

    @pytest.mark.parametrize(
        "country, currency, lower_maturity, upper_maturity",
        [("Denmark", "DKK", datetime(2040, 1, 1), datetime(2050, 1, 1))],
    )
    def test_search_bonds_dict(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        country: str,
        currency: str,
        lower_maturity: datetime,
        upper_maturity: datetime,
    ) -> None:
        """Check if dictionary results are correct."""
        bond_results = na_service.search_bonds(
            country=country,
            currency=currency,
            lower_maturity=lower_maturity,
            upper_maturity=upper_maturity,
        )

        assert bond_results.__len__() > 0
        bond_isin = bond_results[0]["ISIN"]
        bond_name = bond_results[0]["Name"]
        assert type(bond_name) == str
        assert type(bond_isin) == str
        assert bond_isin.__len__() == 12

    @pytest.mark.parametrize(
        "country, currency, lower_maturity, upper_maturity",
        [("Denmark", "DKK", datetime(2040, 1, 1), datetime(2050, 1, 1))],
    )
    def test_search_bonds_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        country: str,
        currency: str,
        lower_maturity: datetime,
        upper_maturity: datetime,
    ) -> None:
        """Check if DataFrame results are correct."""
        bond_results = na_service.search_bonds(
            country=country,
            currency=currency,
            lower_maturity=lower_maturity,
            upper_maturity=upper_maturity,
            as_df=True,
        )

        assert bond_results.__len__() > 0
        bond_isin = bond_results.ISIN[0]
        bond_name = bond_results.Name[0]
        assert type(bond_name) == str
        assert type(bond_isin) == str
        assert bond_isin.__len__() == 12

    @pytest.mark.parametrize(
        "country, currency, asset_types",
        [
            (
                "Denmark",
                "DKK",
                AssetType.NonCallableBond,
            ),
            (
                "denMARK",
                "dkK",
                "Non-callABLE Bond",
            ),
            (
                "Denmark",
                "DKK",
                [AssetType.NonCallableBond],
            ),
            (
                "Denmark",
                "DKK",
                ["Non-callable BOND"],
            ),
            (
                "Denmark",
                "DKK",
                [AssetType.NonCallableBond, "frN"],
            ),
        ],
    )
    def test_search_bonds_asset_type_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        country: str,
        currency: str,
        asset_types: Any,
    ) -> None:
        """Check if asset_type input types work as expected."""
        bond_results = na_service.search_bonds(
            country=country,
            currency=currency,
            asset_types=asset_types,
        )

        assert bond_results.__len__() > 0
        bond_isin = bond_results[0]["ISIN"]
        bond_name = bond_results[0]["Name"]
        assert type(bond_name) == str
        assert type(bond_isin) == str
        assert bond_isin.__len__() == 12

    @pytest.mark.parametrize(
        "country, currency, amortisation_types",
        [
            (
                "Denmark",
                "DKK",
                AmortisationType.Bullet,
            ),
            (
                "Denmark",
                "DKK",
                "bullET",
            ),
        ],
    )
    def test_search_bonds_amortisation_types_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        country: str,
        currency: str,
        amortisation_types: Union[
            AmortisationType,
            str,
        ],
    ) -> None:
        """Check if amortisation_type input types work as expected."""
        bond_results = na_service.search_bonds(
            country=country,
            currency=currency,
            amortisation_type=amortisation_types,
        )

        assert bond_results.__len__() > 0
        bond_isin = bond_results[0]["ISIN"]
        bond_name = bond_results[0]["Name"]
        assert type(bond_name) == str
        assert type(bond_isin) == str
        assert bond_isin.__len__() == 12

    @pytest.mark.parametrize(
        "issuers",
        [
            Issuers.Aareal_Bank_AG,
            "Aareal Bank aG",
        ],
    )
    def test_search_bonds_issuers_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        issuers: Any,
    ) -> None:
        """Check if issuers input types work as expected."""
        bond_results = na_service.search_bonds(
            issuers=issuers,
        )

        assert bond_results.__len__() > 0
        bond_isin = bond_results[0]["ISIN"]
        bond_name = bond_results[0]["Name"]
        assert type(bond_name) == str
        assert type(bond_isin) == str
        assert bond_isin.__len__() == 12

    @pytest.mark.parametrize(
        "country, currency, capital_centres",
        [
            (
                "Denmark",
                "DKK",
                CapitalCentres.DLR_B,
            ),
            (
                "Denmark",
                "DKK",
                "b (dlR)",
            ),
            (
                "Denmark",
                "DKK",
                [CapitalCentres.DLR_General, "b (dlR)"],
            ),
        ],
    )
    def test_search_bonds_capital_centres_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        country: str,
        currency: str,
        capital_centres: Any,
    ) -> None:
        """Check if capital_centre input types work as expected."""
        bond_results = na_service.search_bonds(
            country=country,
            currency=currency,
            capital_centres=capital_centres,
            dmb=True,
        )

        assert bond_results.__len__() > 0
        bond_isin = bond_results[0]["ISIN"]
        bond_name = bond_results[0]["Name"]
        assert type(bond_name) == str
        assert type(bond_isin) == str
        assert bond_isin.__len__() == 12

    @pytest.mark.parametrize(
        "country, currency, capital_centre_types",
        [
            (
                "Denmark",
                "DKK",
                CapitalCentreTypes.JCB,
            ),
            (
                "Denmark",
                "DKK",
                "jcB",
            ),
            (
                "Denmark",
                "DKK",
                [CapitalCentreTypes.JCB, "SdrO"],
            ),
        ],
    )
    def test_search_bonds_capital_centre_types_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        country: str,
        currency: str,
        capital_centre_types: Any,
    ) -> None:
        """Check if capital_centre_types input types work as expected."""
        bond_results = na_service.search_bonds(
            country=country,
            currency=currency,
            capital_centre_types=capital_centre_types,
            dmb=True,
        )

        assert bond_results.__len__() > 0
        bond_isin = bond_results[0]["ISIN"]
        bond_name = bond_results[0]["Name"]
        assert type(bond_name) == str
        assert type(bond_isin) == str
        assert bond_isin.__len__() == 12
