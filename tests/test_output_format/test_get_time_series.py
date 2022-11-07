from datetime import datetime
from typing import List, Union
import warnings


from dateutil import relativedelta  # type: ignore
import pandas as pd
import pytest

from nordea_analytics import (
    NordeaAnalyticsService,
    TimeSeriesKeyFigureName,
)


@pytest.fixture
def from_date() -> datetime:
    """From date for Time Series and Curve Time Series test."""
    return datetime(2021, 1, 1)


@pytest.fixture
def to_date() -> datetime:
    """From date for Time Series and Curve Time Series test."""
    return datetime(2022, 2, 1)


class TestTimeSeries:
    """Test class for retrieving bond key figures."""

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                ["DK0002000421", "DK0002044551"],
                TimeSeriesKeyFigureName.Spread,
            )
        ],
    )
    def test_time_series_dict(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        symbols: List[str],
        keyfigures: Union[
            str,
            TimeSeriesKeyFigureName,
            List[str],
            List[TimeSeriesKeyFigureName],
            List[Union[str, TimeSeriesKeyFigureName]],
        ],
    ) -> None:
        """Check if dictionary results are correct."""
        time_series_results = na_service.get_time_series(
            symbols, keyfigures, from_date, to_date
        )

        expected_symbols = symbols
        actual_symbols = list(time_series_results.keys())
        assert sorted(actual_symbols) == sorted(expected_symbols)

        _keyfigures = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        expected_keyfigures: list = [
            kf.name if isinstance(kf, TimeSeriesKeyFigureName) else kf.lower()
            for kf in _keyfigures
        ]

        for key in time_series_results:
            actual_keyfigures = list(time_series_results[key].keys())
            assert sorted(actual_keyfigures) == sorted(expected_keyfigures)

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                ["DK0002000421", "DK0002044551"],
                TimeSeriesKeyFigureName.Spread,
            )
        ],
    )
    def test_time_series_df(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        symbols: List[str],
        keyfigures: Union[
            str,
            TimeSeriesKeyFigureName,
            List[str],
            List[TimeSeriesKeyFigureName],
            List[Union[str, TimeSeriesKeyFigureName]],
        ],
    ) -> None:
        """Check if DataFrame results are correct."""
        time_series_results = na_service.get_time_series(
            symbols, keyfigures, from_date, to_date, as_df=True
        )

        expected_symbols = symbols
        actual_symbols = set(pd.DataFrame(time_series_results)["Symbol"])
        assert sorted(actual_symbols) == sorted(expected_symbols)

        _keyfigures = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        expected_keyfigures: list = [
            kf.name if isinstance(kf, TimeSeriesKeyFigureName) else kf.lower()
            for kf in _keyfigures
        ]

        actual_keyfigures = list(time_series_results)
        actual_keyfigures.remove("Symbol")
        actual_keyfigures.remove("Date")
        assert sorted(actual_keyfigures) == sorted(expected_keyfigures)

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                ["DK0002000421", "DK0002044551"],
                TimeSeriesKeyFigureName.Spread,
            ),
            (
                ["DK0002000421", "DK0002044551"],
                "spreaD",  # to check that output key is also with capitalised D
            ),
        ],
    )
    def test_time_series_keyfigures_inputs(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        symbols: List[str],
        keyfigures: Union[
            str,
            TimeSeriesKeyFigureName,
            List[str],
            List[TimeSeriesKeyFigureName],
            List[Union[str, TimeSeriesKeyFigureName]],
        ],
    ) -> None:
        """Check if keyfigures input types work as expected."""
        time_series_results = na_service.get_time_series(
            symbols, keyfigures, from_date, to_date
        )

        expected_symbols = symbols
        actual_symbols = list(time_series_results.keys())
        assert sorted(actual_symbols) == sorted(expected_symbols)

        _keyfigures = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        expected_keyfigures: list = [
            kf.name if isinstance(kf, TimeSeriesKeyFigureName) else kf
            for kf in _keyfigures
        ]

        for key in time_series_results:
            actual_keyfigures = list(time_series_results[key].keys())
            assert sorted(actual_keyfigures) == sorted(expected_keyfigures)

    @pytest.mark.parametrize(
        "symbols_partial_spread_data, keyfigures",
        [
            (
                ["DK0009408601", "DK0002030337"],
                TimeSeriesKeyFigureName.Spread,
            )
        ],
    )
    def test_time_series_partial_data_is_returned(
        self,
        na_service: NordeaAnalyticsService,
        symbols_partial_spread_data: List[str],
        keyfigures: List[Union[str, TimeSeriesKeyFigureName]],
    ) -> None:
        """Check if available key figures are returned despite some ISINs having no key figures available."""
        with warnings.catch_warnings(record=True):
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            # Causes warning to trigger

            from_date = datetime(2021, 7, 6)
            to_date = datetime(2021, 7, 6)
            time_series_results = na_service.get_time_series(
                symbols_partial_spread_data, keyfigures, from_date, to_date
            )

            sorted_symbols = sorted(symbols_partial_spread_data)
            symbol_with_data = sorted_symbols[0]
            symbol_no_data = sorted_symbols[1]

            assert symbol_no_data not in time_series_results

            assert symbol_with_data in time_series_results

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                ["DK0009408601"],
                TimeSeriesKeyFigureName.Spread,
            )
        ],
    )
    def test_time_series_no_data_is_returned(
        self,
        na_service: NordeaAnalyticsService,
        symbols: List[str],
        keyfigures: List[Union[str, TimeSeriesKeyFigureName]],
    ) -> None:
        """Check if available key figures are returned despite some ISINs having no key figures available."""
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            # Causes warning to trigger

            from_date = datetime(2021, 7, 4)
            to_date = datetime(2021, 7, 4)
            time_series_results = na_service.get_time_series(
                symbols, keyfigures, from_date, to_date
            )

            sorted_symbols = sorted(symbols)
            symbol_no_data = sorted_symbols[0]

            assert symbol_no_data not in time_series_results

            assert len(w) > 0
            assert (
                f"Failed to retrieve 'spread' for '{symbol_no_data}"
                in w[0].message.args[0]  # type: ignore
            )

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                [
                    "US91282CBG50",
                    "US912810EP94",
                    "US912828UN88",
                    "US912828Z864",
                    "US9128284A52",
                    "US912828P790",
                    "US91282CBN02",
                    "US912828ZD51",
                    "XS1791714147",
                    "US9128284D91",
                    "US912828Q293",
                    "US91282CBU45",
                    "US9128284H06",
                    "US912828ZH65",
                    "US9128284L18",
                    "US912828R283",
                    "US91282CBX83",
                    "US912828VB32",
                    "US912828ZP81",
                    "XS1779710901",
                    "US9128284S60",
                    "US912828R697",
                    "US91282CCD11",
                    "US912828ZU76",
                    "US9128284U17",
                    "US912828S356",
                    "US91282CCK53",
                    "US912828VM96",
                    "US912828ZY98",
                    "US912828S927",
                    "US912828Y610",
                    "US91282CCN92",
                    "US912828YF19",
                    "XS0320606840",
                    "US9128282W90",
                    "US912828L575",
                    "US91282CAN11",
                    "US912796M895",
                    "US912828YK04",
                    "US9128283C28",
                    "US912828M490",
                    "US91282CAR25",
                    "US912796N968",
                    "US912810EN47",
                    "US912828TY62",
                    "US912828M805",
                    "US91282CAX92",
                    "US912796P948",
                    "US912828YW42",
                    "XS2160873480",
                    "US912796R274",
                    "US912828N308",
                    "US91282CBD20",
                    "US912828UH11",
                    "US912828Z294",
                    "US912796S348",
                    "US9128283U26",
                    "US912828P386",
                ],
                [
                    TimeSeriesKeyFigureName.Spread,
                ],
            )
        ],
    )
    def test_time_series_exceeding_limit_symbols_and_keyfigures_should_succeed(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        symbols: List[str],
        keyfigures: List[Union[str, TimeSeriesKeyFigureName]],
    ) -> None:
        """Check if result for all bonds is returned despite being over the symbol limit."""
        time_series_results = na_service.get_time_series(
            symbols, keyfigures, from_date, to_date
        )

        assert len(time_series_results) == len(symbols)

    @pytest.mark.parametrize(
        "symbols, keyfigures, from_date, to_date",
        [
            (
                "DK0002000421",
                TimeSeriesKeyFigureName.Spread,
                datetime(2010, 1, 1),
                datetime(2022, 1, 1),
            )
        ],
    )
    def test_time_series_exceeding_limit_of_10_years_should_succeed(
        self,
        na_service: NordeaAnalyticsService,
        from_date: datetime,
        to_date: datetime,
        symbols: List[str],
        keyfigures: Union[
            str,
            TimeSeriesKeyFigureName,
            List[str],
            List[TimeSeriesKeyFigureName],
            List[Union[str, TimeSeriesKeyFigureName]],
        ],
    ) -> None:
        """Check if result for all bonds is returned despite being over the symbol limit."""
        time_series_results = na_service.get_time_series(
            symbols, keyfigures, from_date, to_date
        )

        keyfigure_key = (
            keyfigures.name
            if isinstance(keyfigures, TimeSeriesKeyFigureName)
            else keyfigures
        )
        dates = time_series_results[symbols][keyfigure_key]["Date"]
        last_date = dates[0]
        first_date = dates[-1]  # last element
        assert relativedelta.relativedelta(last_date, first_date).years > 10
