from datetime import datetime
from typing import List, Union

from numpy import float64
import pytest

from nordea_analytics import NordeaAnalyticsService


class TestIndexComposition:
    """Test class for retrieving index composition data."""

    @pytest.mark.parametrize(
        "indices",
        [
            "DK Govt",
        ],
    )
    def test_index_composition_dict(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        indices: Union[str, List[str]],
    ) -> None:
        """Check if dictionary results are correct."""
        indices_results = na_service.get_index_composition(
            indices=indices,
            calc_date=anchor,
        )

        _indices_input: list = indices if isinstance(indices, list) else [indices]
        for index_key in _indices_input:
            assert index_key in indices_results

            index_composition = indices_results[index_key]
            assert index_composition.__len__() > 0

            isins = index_composition["ISIN"]
            assert isins.__len__() > 0
            assert isinstance(isins[0], str)

            names = index_composition["Name"]
            assert names.__len__() > 0
            assert isinstance(names[0], str)

            nominal_amounts = index_composition["Nominal_Amount"]
            assert nominal_amounts.__len__() > 0
            assert isinstance(nominal_amounts[0], float)

            nominal_weights = index_composition["Nominal_Weight"]
            assert nominal_weights.__len__() > 0
            assert isinstance(nominal_weights[0], float)

            market_amounts = index_composition["Market_Amount"]
            assert market_amounts.__len__() > 0
            assert isinstance(market_amounts[0], float)

            market_weights = index_composition["Market_Weight"]
            assert market_weights.__len__() > 0
            assert isinstance(market_weights[0], float)

    @pytest.mark.parametrize(
        "indices",
        [
            "DK Govt",
        ],
    )
    def test_index_composition_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        indices: Union[str, List[str]],
    ) -> None:
        """Check if DataFrame results are correct."""
        indices_results = na_service.get_index_composition(
            indices=indices, calc_date=anchor, as_df=True
        )

        _indices_input = indices if isinstance(indices, list) else [indices]

        for index_key in _indices_input:
            index_results = indices_results[indices_results["Index"].isin([index_key])]
            assert index_results.__len__() > 0

            assert index_results.ISIN.__len__() > 0
            assert type(index_results.ISIN[0]) == str

            assert index_results.Name.__len__() > 0
            assert type(index_results.Name[0]) == str

            assert index_results.Nominal_Amount.__len__() > 0
            assert type(index_results.Nominal_Amount[0]) == float64

            assert index_results.Nominal_Weight.__len__() > 0
            assert type(index_results.Nominal_Weight[0]) == float64

            assert index_results.Market_Amount.__len__() > 0
            assert type(index_results.Market_Amount[0]) == float64

            assert index_results.Market_Weight.__len__() > 0
            assert type(index_results.Market_Weight[0]) == float64

    @pytest.mark.parametrize("indices", ["DK Govt", ["DK gOVT", "DK Mtg Callable"]])
    def test_index_composition_indices_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        indices: Union[str, List[str]],
    ) -> None:
        """Check if indices input types work as expected."""
        indices_results = na_service.get_index_composition(
            indices=indices,
            calc_date=anchor,
        )

        _indices = indices if isinstance(indices, list) else [indices]

        for index_key in _indices:
            assert index_key in indices_results

            index_composition = indices_results[index_key]
            assert index_composition.__len__() > 0

            isins = index_composition["ISIN"]
            assert isins.__len__() > 0
            assert type(isins[0]) == str

            names = index_composition["Name"]
            assert names.__len__() > 0
            assert type(names[0]) == str

            nominal_amounts = index_composition["Nominal_Amount"]
            assert nominal_amounts.__len__() > 0
            assert type(nominal_amounts[0]) == float

            nominal_weights = index_composition["Nominal_Weight"]
            assert nominal_weights.__len__() > 0
            assert type(nominal_weights[0]) == float

            market_amounts = index_composition["Market_Amount"]
            assert market_amounts.__len__() > 0
            assert type(market_amounts[0]) == float

            market_weights = index_composition["Market_Weight"]
            assert market_weights.__len__() > 0
            assert type(market_weights[0]) == float

    # @pytest.mark.parametrize(
    #     "indices, calc_date",
    #     (
    #         [
    #             "DK Govt",
    #             datetime(2014, 5, 23)
    #         ]
    #     ),
    # )
    @pytest.mark.parametrize(
        "indices, calc_date",
        [("DK Govt", datetime(2011, 10, 3))],
    )
    def test_index_composition_missing_market_nominals(
        self,
        na_service: NordeaAnalyticsService,
        calc_date: datetime,
        indices: Union[str, List[str]],
    ) -> None:
        """Check if dictionary results are correct."""
        indices_results = na_service.get_index_composition(
            indices=indices,
            calc_date=calc_date,
        )

        _indices_input: list = indices if isinstance(indices, list) else [indices]
        for index_key in _indices_input:
            assert index_key in indices_results

            index_composition = indices_results[index_key]
            assert index_composition.__len__() > 0

            isins = index_composition["ISIN"]
            assert isins.__len__() > 0
            assert isinstance(isins[0], str)

            names = index_composition["Name"]
            assert names.__len__() > 0
            assert isinstance(names[0], str)

            nominal_amounts = index_composition["Nominal_Amount"]
            assert nominal_amounts.__len__() > 0
            assert isinstance(nominal_amounts[0], float)

            nominal_weights = index_composition["Nominal_Weight"]
            assert nominal_weights.__len__() > 0
            assert isinstance(nominal_weights[0], float)

            market_amounts = index_composition["Market_Amount"]
            assert all(x is None for x in market_amounts)

            assert "Market_Weight" not in index_composition
