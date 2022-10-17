from datetime import datetime
from typing import Union
import warnings

import pandas as pd
import pytest

from nordea_analytics import (
    CurveDefinitionName,
    CurveName,
    NordeaAnalyticsService,
)
from nordea_analytics.nalib.exceptions import ApiServerError


@pytest.fixture
def anchor() -> datetime:
    """Value date for tests."""
    return datetime(2022, 8, 1)


class TestGetCurveDefinition:
    """Test class for get curve."""

    @pytest.mark.parametrize(
        "curve",
        [
            CurveName.DKKMTGNYK,
        ],
    )
    def test_curve_definition_dict(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curve: Union[str, CurveDefinitionName, CurveName],
    ) -> None:
        """Check if dictionary results are correct."""
        curve_results = na_service.get_curve_definition(
            curve=curve,
            calc_date=anchor,
        )

        curve_string = self.curve_to_string(curve)

        assert curve_string in curve_results

        curve_definition: dict = curve_results[curve_string]
        assert curve_definition.__len__() > 0
        quote = list(curve_definition.values())[0]["Quote"]
        weight = list(curve_definition.values())[0]["Weight"]
        maturity = list(curve_definition.values())[0]["Maturity"]

        assert isinstance(quote, float)
        assert isinstance(weight, int)
        assert isinstance(maturity, datetime)

    @pytest.mark.parametrize(
        "curve",
        [CurveName.DKKMTGNYK],
    )
    def test_curve_definition_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curve: Union[str, CurveDefinitionName, CurveName],
    ) -> None:
        """Check if Data Frame results are correct."""
        curve_results = na_service.get_curve_definition(
            curve=curve, calc_date=anchor, as_df=True
        )

        curve_string = self.curve_to_string(curve)

        assert curve_string in curve_results.index

        number_of_underlying_bonds = curve_results.Name.__len__()
        assert number_of_underlying_bonds > 0
        quote = curve_results.Quote[0]
        weight = curve_results.Weight[0]
        maturity = curve_results.Maturity[0]

        assert isinstance(quote, float)
        assert isinstance(weight, int)
        assert isinstance(maturity, pd.Timestamp)

    @pytest.mark.parametrize(
        "curve",
        [
            CurveName.DKKGOV,
            CurveDefinitionName.DKKGOV,
            "DKKGov",
        ],
    )
    def test_curve_definition_curve_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curve: Union[str, CurveDefinitionName, CurveName],
    ) -> None:
        """Check if curve input types work as expected."""
        curve_results = na_service.get_curve_definition(
            curve=curve,
            calc_date=anchor,
        )

        curve_string = self.curve_to_string(curve)

        assert curve_string in curve_results

        curve_definition: dict = curve_results[curve_string]
        assert curve_definition.__len__() > 0
        quote = list(curve_definition.values())[0]["Quote"]
        weight = list(curve_definition.values())[0]["Weight"]
        maturity = list(curve_definition.values())[0]["Maturity"]

        assert isinstance(quote, float)
        assert isinstance(weight, int)
        assert isinstance(maturity, datetime)

    @pytest.mark.parametrize(
        "curve",
        [
            CurveName.DKKMTGNYK,
            CurveDefinitionName.DKKMTGNYK,
            "DKKMTGNYKSOFTBLT",
        ],
    )
    def test_curve_definition_curve_input_output_match(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curve: Union[str, CurveDefinitionName, CurveName],
    ) -> None:
        """Check if curve input and output match or correspond to Enum.name value."""
        curve_results = na_service.get_curve_definition(
            curve=curve,
            calc_date=anchor,
        )

        curve_string = self.curve_to_string(curve)

        assert curve_string in curve_results

    @pytest.mark.parametrize(
        "curve",
        [
            CurveName.DKKSWAP_Disc_OIS,
        ],
    )
    def test_curve_definition_infinity_curve_should_fail(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        curve: Union[str, CurveDefinitionName, CurveName],
    ) -> None:
        """Check if Infinity curves fail as expected."""
        with warnings.catch_warnings(record=True):
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            try:
                curve_results = na_service.get_curve_definition(  # NOQA
                    curve=curve,
                    calc_date=anchor,
                )
            except ApiServerError as e:
                assert e.args[0].__contains__("restricted")

    @staticmethod
    def curve_to_string(curve: Union[CurveName, CurveDefinitionName, str]) -> str:
        """Converts CurveName and CurveDefinitionName to string."""
        return (
            CurveName(curve).name
            if isinstance(curve, CurveName)
            else CurveDefinitionName(curve).name
            if isinstance(curve, CurveDefinitionName)
            else curve
        )
