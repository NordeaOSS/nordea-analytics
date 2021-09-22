from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any, List, Union

import pytest

from nordea_analytics.bond_key_figure_name import BondKeyFigureName
from nordea_analytics.curve_variable_names import (
    CurveType,
    SpotForward,
    TimeConvention,
)
from tests import NordeaAnalyticsServiceFile
from tests.util import load_and_compare_dfs

DUMP_DATA = False


@pytest.fixture
def na_service() -> NordeaAnalyticsServiceFile:
    """NordeaAnaLyticsService test class."""
    return NordeaAnalyticsServiceFile()


@pytest.fixture
def anchor() -> datetime:
    """Value date for tests."""
    return datetime(2021, 7, 6)


@pytest.fixture
def result_path() -> str:
    """Path where expected results are saved."""
    return str(Path(__file__).parent / "data" / "expected_results")


@pytest.fixture
def isins() -> List[str]:
    """ISINs used for testing."""
    return ["DK0002044551", "DE0001141794"]


@pytest.fixture
def keyfigures() -> List[Any]:
    """Key Figures for BondKeyFigures and TimeSeries tests."""
    return [
        "Quote",
        "Yield",
        "Modduration",
        "spread",
        "accint",
        BondKeyFigureName.BPVP,
        BondKeyFigureName.CVXP,
    ]


@pytest.fixture
def index() -> List[str]:
    """Incices for IndexComposition test."""
    return ["DK Mtg Callable", "DK Govt"]


@pytest.fixture
def from_date() -> datetime:
    """From date for Time Series and Curve Time Series test."""
    return datetime(2021, 1, 1)


@pytest.fixture
def tenors() -> List[float]:
    """Tenors for Curve Time Series test."""
    return [0.5, 1]


class TestBondKeyFigures:
    """Test class for retrieving bond key figures."""

    def test_get_bond_key_figures_dict(
        self,
        na_service: NordeaAnalyticsServiceFile,
        anchor: datetime,
        result_path: str,
        isins: List[str],
        keyfigures: Union[
            List[str], List[BondKeyFigureName], List[Union[str, BondKeyFigureName]]
        ],
    ) -> None:
        """Check if dictionary results are correct."""
        bond_key_figures = na_service.get_bond_key_figures(isins, keyfigures, anchor)

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    anchor.strftime("%d%m%y") + "_key_figures.txt",
                ),
                "w+",
            )
            json.dump(bond_key_figures, expected_file)

        expected_file = open(
            os.path.join(result_path, anchor.strftime("%d%m%y") + "_key_figures.txt"),
            "r",
        )
        expected_result = json.load(expected_file)

        assert bond_key_figures == expected_result

    def test_get_bond_key_figures_df(
        self,
        na_service: NordeaAnalyticsServiceFile,
        anchor: datetime,
        result_path: str,
        isins: List[str],
        keyfigures: List[str],
    ) -> None:
        """Check if DataFrame results are correct."""
        df = na_service.get_bond_key_figures(isins, keyfigures, anchor, as_df=True)

        load_and_compare_dfs(
            df,
            os.path.join(
                result_path, anchor.strftime("%d%m%y") + "_key_figures_df.csv"
            ),
            index_col=0,
            dump_data=DUMP_DATA,
        )


class TestIndexComposition:
    """Test class for retrieving index Composition."""

    def test_get_index_comopsition_dict(
        self,
        na_service: NordeaAnalyticsServiceFile,
        anchor: datetime,
        result_path: str,
        index: List[str],
    ) -> None:
        """Check if dictionary results are correct."""
        index_composition = na_service.get_index_composition(index, anchor)
        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    anchor.strftime("%d%m%y") + "_index_composition.txt",
                ),
                "w+",
            )
            json.dump(index_composition, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                anchor.strftime("%d%m%y") + "_index_composition.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert index_composition == expected_result

    def test_get_index_comopsition_df(
        self,
        na_service: NordeaAnalyticsServiceFile,
        anchor: datetime,
        result_path: str,
        index: List[str],
    ) -> None:
        """Check if DataFrame results are correct."""
        df = na_service.get_index_composition(index, anchor, as_df=True)

        load_and_compare_dfs(
            df,
            os.path.join(
                result_path,
                anchor.strftime("%d%m%y") + "_index_composition_df.csv",
            ),
            index_col=0,
            dump_data=DUMP_DATA,
            reset_index=True,
        )


class TestTimeSeries:
    """Test class for retrieving key figure time series."""

    def test_get_time_series_dict(
        self,
        na_service: NordeaAnalyticsServiceFile,
        anchor: datetime,
        from_date: datetime,
        result_path: str,
        isins: List[str],
        keyfigures: Union[
            List[str], List[BondKeyFigureName], List[Union[str, BondKeyFigureName]]
        ],
    ) -> None:
        """Check if dictionary results are correct."""
        time_series = na_service.get_time_series(isins, keyfigures, from_date, anchor)
        # change dateformat so it can be saved
        for isin in time_series:
            for keyfigure in time_series[isin]:
                time_series[isin][keyfigure]["Date"] = [
                    x.isoformat() for x in time_series[isin][keyfigure]["Date"]
                ]

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    from_date.strftime("%d%m%y")
                    + "-"
                    + anchor.strftime("%d%m%y")
                    + "_time_series.txt",
                ),
                "w+",
            )
            json.dump(time_series, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                from_date.strftime("%d%m%y")
                + "-"
                + anchor.strftime("%d%m%y")
                + "_time_series.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert time_series == expected_result

    def test_get_time_series_df(
        self,
        na_service: NordeaAnalyticsServiceFile,
        anchor: datetime,
        from_date: datetime,
        result_path: str,
        isins: List[str],
        keyfigures: List[str],
    ) -> None:
        """Check if DataFrame results are correct."""
        df = na_service.get_time_series(
            isins, keyfigures, from_date, anchor, as_df=True
        )

        df.Date = df.Date.apply(str)
        load_and_compare_dfs(
            df,
            os.path.join(
                result_path,
                from_date.strftime("%d%m%y")
                + "-"
                + anchor.strftime("%d%m%y")
                + "_time_series.csv",
            ),
            index_col=0,
            dump_data=DUMP_DATA,
            reset_index=True,
        )


class TestCurveTimeSeries:
    """Test class for retrieving curve time series."""

    @pytest.mark.parametrize(
        "curve, curve_type, time_convention, spot_forward, forward_tenor",
        [
            (
                "DKKSWAP",
                CurveType.Bootstrap,
                TimeConvention.Act365,
                SpotForward.Forward,
                2,
            ),
            (
                "DKKGOV",
                CurveType.ParCurve,
                TimeConvention.TC_30360,
                SpotForward.Spot,
                None,
            ),
        ],
    )
    def test_get_curve_time_series_dict(
        self,
        na_service: NordeaAnalyticsServiceFile,
        anchor: datetime,
        from_date: datetime,
        result_path: str,
        curve: str,
        curve_type: Union[str, CurveType],
        time_convention: Union[str, TimeConvention],
        tenors: List[float],
        spot_forward: Union[str, SpotForward],
        forward_tenor: Union[float, None],
    ) -> None:
        """Check if dictionary results are correct."""
        curve_time_series = na_service.get_curve_time_series(
            curve,
            from_date,
            anchor,
            curve_type,
            time_convention,
            tenors,
            spot_forward,
            forward_tenor,
        )
        # change dateformat so it can be saved
        for curve_tenor in curve_time_series:
            curve_time_series[curve_tenor]["Date"] = [
                x.isoformat() for x in curve_time_series[curve_tenor]["Date"]
            ]

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    curve
                    + "_"
                    + from_date.strftime("%d%m%y")
                    + "-"
                    + anchor.strftime("%d%m%y")
                    + "_curve_time_series.txt",
                ),
                "w+",
            )
            json.dump(curve_time_series, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                curve
                + "_"
                + from_date.strftime("%d%m%y")
                + "-"
                + anchor.strftime("%d%m%y")
                + "_curve_time_series.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert curve_time_series == expected_result

    @pytest.mark.parametrize(
        "curve, curve_type, time_convention, spot_forward, forward_tenor",
        [
            (
                "DKKSWAP",
                CurveType.Bootstrap,
                TimeConvention.Act365,
                SpotForward.Forward,
                2,
            ),
            (
                "DKKGOV",
                CurveType.ParCurve,
                TimeConvention.TC_30360,
                SpotForward.Spot,
                None,
            ),
        ],
    )
    def test_get_curve_time_series_df(
        self,
        na_service: NordeaAnalyticsServiceFile,
        anchor: datetime,
        from_date: datetime,
        result_path: str,
        curve: str,
        curve_type: Union[str, CurveType],
        time_convention: Union[str, TimeConvention],
        tenors: List[float],
        spot_forward: Union[str, SpotForward],
        forward_tenor: Union[float, None],
    ) -> None:
        """Check if dictionary results are correct."""
        df = na_service.get_curve_time_series(
            curve,
            from_date,
            anchor,
            curve_type,
            time_convention,
            tenors,
            spot_forward,
            forward_tenor,
            as_df=True,
        )
        df.Date = df.Date.apply(str)
        load_and_compare_dfs(
            df,
            os.path.join(
                result_path,
                curve
                + "_"
                + from_date.strftime("%d%m%y")
                + "-"
                + anchor.strftime("%d%m%y")
                + "_curve_time_series.csv",
            ),
            index_col=0,
            dump_data=DUMP_DATA,
            reset_index=True,
        )
