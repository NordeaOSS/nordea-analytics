from datetime import datetime
import json
import os
from pathlib import Path
from typing import List, Union

import pytest

from nordea_analytics.curve_variable_names import (
    CurveName,
    CurveType,
    SpotForward,
    TimeConvention,
)
from nordea_analytics.key_figure_names import (
    BondKeyFigureName,
    CalculatedBondKeyFigureName,
    TimeSeriesKeyFigureName,
)
from nordea_analytics.search_bond_names import (
    AmortisationType,
    AssetType,
    CapitalCentres,
    CapitalCentreTypes,
    Issuers,
)
from tests import NordeaAnalyticsServiceTest
from tests.util import load_and_compare_dfs

DUMP_DATA = False


@pytest.fixture
def na_service() -> NordeaAnalyticsServiceTest:
    """NordeaAnaLyticsService test class."""
    return NordeaAnalyticsServiceTest()


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
def keyfigures() -> List[Union[str, BondKeyFigureName]]:
    """Key Figures for BondKeyFigures test."""
    return [
        "Quote",
        "Yield",
        "Modduration",
        "accint",
        BondKeyFigureName.BPVP,
        BondKeyFigureName.CVXP,
        BondKeyFigureName.OAModifiedDuration,
    ]


@pytest.fixture
def timeseries_keyfigures() -> List[Union[str, TimeSeriesKeyFigureName]]:
    """Key Figures for TimeSeries tests."""
    return [
        "Quote",
        "Yield",
        "Modduration",
        "accint",
        TimeSeriesKeyFigureName.BPVP,
        TimeSeriesKeyFigureName.CVXP,
        TimeSeriesKeyFigureName.ModifiedDuration,
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
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        result_path: str,
        isins: List[str],
        keyfigures: List[Union[str, BondKeyFigureName]],
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
            os.path.join(
                result_path,
                anchor.strftime("%d%m%y") + "_key_figures.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert bond_key_figures == expected_result

    def test_get_bond_key_figures_df(
        self,
        na_service: NordeaAnalyticsServiceTest,
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


# @pytest.mark.skip("skip until I figure out why market value changes daily")
class TestIndexComposition:
    """Test class for retrieving index Composition."""

    def test_get_index_composition_dict(
        self,
        na_service: NordeaAnalyticsServiceTest,
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
        na_service: NordeaAnalyticsServiceTest,
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
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        from_date: datetime,
        result_path: str,
        isins: List[str],
        timeseries_keyfigures: List[Union[str, TimeSeriesKeyFigureName]],
    ) -> None:
        """Check if dictionary results are correct."""
        time_series = na_service.get_time_series(
            isins, timeseries_keyfigures, from_date, anchor
        )
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
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        from_date: datetime,
        result_path: str,
        isins: List[str],
        timeseries_keyfigures: List[str],
    ) -> None:
        """Check if DataFrame results are correct."""
        df = na_service.get_time_series(
            isins, timeseries_keyfigures, from_date, anchor, as_df=True
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
                CurveName.EURSWAP_Disc_ESTR,
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
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        from_date: datetime,
        result_path: str,
        curve: Union[str, CurveName],
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
            tenors,
            curve_type,
            time_convention,
            spot_forward,
            forward_tenor,
        )
        # change dateformat so it can be saved
        for curve_tenor in curve_time_series:
            curve_time_series[curve_tenor]["Date"] = [
                x.isoformat() for x in curve_time_series[curve_tenor]["Date"]
            ]

        curve_name = curve if type(curve) == str else curve.name  # type:ignore
        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    curve_name
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
                curve_name
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
                CurveName.USDSWAP_Libor,
                CurveType.Bootstrap,
                TimeConvention.Act365,
                SpotForward.Forward,
                2,
            ),
            (
                CurveName.DKKGOV,
                CurveType.ParCurve,
                TimeConvention.TC_30360,
                SpotForward.Spot,
                None,
            ),
        ],
    )
    def test_get_curve_time_series_df(
        self,
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        from_date: datetime,
        result_path: str,
        curve: Union[str, CurveName],
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
            tenors,
            curve_type,
            time_convention,
            spot_forward,
            forward_tenor,
            as_df=True,
        )
        df.Date = df.Date.apply(str)
        curve_name = curve if type(curve) == str else curve.name  # type:ignore
        load_and_compare_dfs(
            df,
            os.path.join(
                result_path,
                curve_name
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

    @pytest.mark.parametrize(
        "curve, curve_type, time_convention, spot_forward, forward_tenor",
        [
            (
                "DKKSWAP",
                CurveType.Bootstrap,
                TimeConvention.Act365,
                SpotForward.Forward,
                None,
            ),
        ],
    )
    def test_check_forward(
        self,
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        from_date: datetime,
        result_path: str,
        curve: Union[str, CurveName],
        curve_type: Union[str, CurveType],
        time_convention: Union[str, TimeConvention],
        tenors: List[float],
        spot_forward: Union[str, SpotForward],
        forward_tenor: Union[float, None],
    ) -> None:
        """Check if dictionary results are correct."""
        try:
            na_service.get_curve_time_series(
                curve,
                from_date,
                anchor,
                tenors,
                curve_type,
                time_convention,
                spot_forward,
                forward_tenor,
            )
            expected_result = False
        except ValueError as e:
            expected_result = (
                e.args[0] == "Forward tenor has to be chosen for forward and "
                "implied forward curves"
            )

        assert expected_result


class TestCurve:
    """Test class for retrieving curve."""

    @pytest.mark.parametrize(
        "curve_name, tenor_frequency, curve_type, time_convention,"
        " spot_forward, forward_tenor",
        [
            (
                CurveName.DKKSWAP,
                0.5,
                CurveType.Bootstrap,
                TimeConvention.Act365,
                SpotForward.Forward,
                2,
            ),
            (
                CurveName.USDSWAP_Fix_3M_OIS,
                0.25,
                CurveType.ParCurve,
                TimeConvention.TC_30360,
                SpotForward.Spot,
                None,
            ),
        ],
    )
    def test_get_curve_dict(
        self,
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        result_path: str,
        curve_name: Union[str, CurveName],
        tenor_frequency: float,
        curve_type: Union[str, CurveType],
        time_convention: Union[str, TimeConvention],
        spot_forward: Union[str, SpotForward],
        forward_tenor: Union[float, None],
    ) -> None:
        """Check if dictionary results are correct."""
        curve = na_service.get_curve(
            curve_name,
            anchor,
            curve_type,
            tenor_frequency,
            time_convention,
            spot_forward,
            forward_tenor,
        )
        curve_name = (
            curve_name if type(curve_name) == str else curve_name.name  # type:ignore
        )
        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    curve_name + "-" + anchor.strftime("%d%m%y") + "_curve.txt",
                ),
                "w+",
            )
            json.dump(curve, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                curve_name + "-" + anchor.strftime("%d%m%y") + "_curve.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert curve == expected_result

    @pytest.mark.parametrize(
        "curve_name, tenor_frequency, curve_type, time_convention,"
        " spot_forward, forward_tenor",
        [
            (
                "DKKSWAP",
                0.5,
                CurveType.Bootstrap,
                TimeConvention.Act365,
                SpotForward.Forward,
                2,
            ),
            (
                "DKKGOV",
                0.25,
                CurveType.ParCurve,
                TimeConvention.TC_30360,
                SpotForward.Spot,
                None,
            ),
        ],
    )
    def test_get_curve_df(
        self,
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        result_path: str,
        curve_name: Union[str, CurveName],
        tenor_frequency: float,
        curve_type: Union[str, CurveType],
        time_convention: Union[str, TimeConvention],
        spot_forward: Union[str, SpotForward],
        forward_tenor: Union[float, None],
    ) -> None:
        """Check if dictionary results are correct."""
        df = na_service.get_curve(
            curve_name,
            anchor,
            curve_type,
            tenor_frequency,
            time_convention,
            spot_forward,
            forward_tenor,
            as_df=True,
        )
        curve_name = (
            curve_name if type(curve_name) == str else curve_name.name  # type:ignore
        )  # type:ignore
        load_and_compare_dfs(
            df,
            os.path.join(
                result_path,
                curve_name + "-" + anchor.strftime("%d%m%y") + "_curve.csv",
            ),
            index_col=0,
            dump_data=DUMP_DATA,
            reset_index=True,
        )


class TestCurveDefinition:
    """Test class for retrieving curve definition."""

    @pytest.mark.parametrize(
        "curve_name",
        [
            ("DKKSWAP"),
            (CurveName.EURSWAP),
        ],
    )
    def test_get_curve_definition_dict(
        self,
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        result_path: str,
        curve_name: Union[str, CurveName],
    ) -> None:
        """Check if dictionary results are correct."""
        curve_definition = na_service.get_curve_definition(curve_name, anchor)
        # change dateformat so it can be saved
        for name in curve_definition:
            for row in curve_definition[name]:
                curve_definition[name][row]["Maturity"] = curve_definition[name][row][
                    "Maturity"
                ].isoformat()

        curve_name = (
            curve_name if type(curve_name) == str else curve_name.name  # type:ignore
        )  # type:ignore
        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    curve_name
                    + "-"
                    + anchor.strftime("%d%m%y")
                    + "_curve_definition.txt",
                ),
                "w+",
            )
            json.dump(curve_definition, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                curve_name + "-" + anchor.strftime("%d%m%y") + "_curve_definition.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert curve_definition == expected_result

    @pytest.mark.parametrize(
        "curve_name",
        [
            (CurveName.DKKSWAP_Fix_1D_OIS),
            (CurveName.SEKGOV),
        ],
    )
    def test_get_curve_definition_df(
        self,
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        result_path: str,
        curve_name: Union[str, CurveName],
    ) -> None:
        """Check if dictionary results are correct."""
        df = na_service.get_curve_definition(curve_name, anchor, as_df=True)
        # change datetime format
        df["Maturity"] = df["Maturity"].dt.strftime("%Y-%m-%d")
        curve_name = (
            curve_name if type(curve_name) == str else curve_name.name  # type:ignore
        )  # type:ignore
        load_and_compare_dfs(
            df,
            os.path.join(
                result_path,
                curve_name + "-" + anchor.strftime("%d%m%y") + "_curve_definition.csv",
            ),
            index_col=0,
            dump_data=DUMP_DATA,
            reset_index=True,
        )


class TestSearchBonds:
    """Test class for search bonds."""

    @pytest.mark.parametrize(
        "dmb, currency, issuers, amortisation_type, asset_type, "
        "upper_maturity, country, is_io, "
        "capital_centres, capital_centre_types",
        [
            (
                False,
                "EUR",
                [Issuers.Stadshypotek_AB],
                None,
                None,
                datetime(2022, 1, 3),
                None,
                None,
                None,
                None,
            ),
            (
                False,
                "USD",
                None,
                AmortisationType.Bullet,
                AssetType.FixToFloatBond,
                None,
                "United States",
                None,
                None,
                None,
            ),
            (
                True,
                "DKK",
                None,
                None,
                None,
                None,
                None,
                True,
                [CapitalCentres.NDA_1, CapitalCentres.RD_General],
                [CapitalCentreTypes.JCB, CapitalCentreTypes.RO],
            ),
        ],
    )
    def test_search_bonds_dict(
        self,
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        result_path: str,
        dmb: bool,
        currency: str,
        issuers: str,
        amortisation_type: AmortisationType,
        asset_type: AssetType,
        upper_maturity: datetime,
        country: str,
        is_io: bool,
        capital_centres: CapitalCentres,
        capital_centre_types: CapitalCentreTypes,
    ) -> None:
        """Check if dictionary results are correct."""
        bonds = na_service.search_bonds(
            dmb=dmb,
            currency=currency,
            issuers=issuers,
            lower_maturity=anchor,
            amortisation_type=amortisation_type,
            asset_types=asset_type,
            upper_maturity=upper_maturity,
            country=country,
            is_io=is_io,
            capital_centres=capital_centres,
            capital_centre_types=capital_centre_types,
        )

        file_name = currency + "_" + anchor.strftime("%d%m%y") + "_search_bonds.txt"

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    file_name,
                ),
                "w+",
            )
            json.dump(bonds, expected_file)

        expected_file = open(os.path.join(result_path, file_name), "r")
        expected_result = json.load(expected_file)
        expected_result = {float(x): expected_result[x] for x in expected_result}

        assert bonds == expected_result

    @pytest.mark.parametrize(
        "dmb, currency, issuers",
        [
            (True, "DKK", [Issuers.UniCredit_Bank_AG]),
            (False, "EUR", [Issuers.Stadshypotek_AB]),
        ],
    )
    def test_search_bonds_df(
        self,
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        result_path: str,
        dmb: bool,
        currency: str,
        issuers: str,
    ) -> None:
        """Check if dictionary results are correct."""
        df = na_service.search_bonds(
            dmb=dmb,
            currency=currency,
            issuers=issuers,
            lower_maturity=anchor,
            as_df=True,
        )
        load_and_compare_dfs(
            df,
            os.path.join(
                result_path,
                "_".join(issuers)
                + "-"
                + anchor.strftime("%d%m%y")
                + "_search_bonds.csv",
            ),
            index_col=0,
            dump_data=True,
            reset_index=False,
        )

    def test_danish_capped_floaters(
        self, na_service: NordeaAnalyticsServiceTest, anchor: datetime
    ) -> None:
        """Hold the statements that we have documented on capped floaters."""
        currency = "DKK"

        all = na_service.search_bonds(
            dmb=True,
            lower_maturity=anchor,
            currency=currency,
            asset_types=AssetType.DanishCappedFloaters,
        )
        only_capped = na_service.search_bonds(
            dmb=True,
            lower_maturity=anchor,
            currency=currency,
            asset_types=AssetType.DanishCappedFloaters,
            upper_coupon=1000,
        )
        only_normal = na_service.search_bonds(
            dmb=True,
            lower_maturity=anchor,
            currency=currency,
            asset_types=AssetType.DanishCappedFloaters,
            lower_coupon=100000,
        )

        assert len(all) == len(only_capped) + len(only_normal)

    # def test_check_input(self, na_service:NordeaAnalyticsServiceTest):
    #     assert na_service.search_bonds(dmb=False, is_io=False)


class TestCalculateBondKeyFigure:
    """Test class for calculate key figure."""

    @pytest.mark.parametrize(
        "isin, bond_key_figure, curves, rates_shift, pp_speed",
        [
            (
                "DK0002000421",
                [CalculatedBondKeyFigureName.Spread, CalculatedBondKeyFigureName.Price],
                CurveName.DKKSWAP_Disc_OIS,
                ["0Y 5", "30Y -5"],
                0.5,
            )
        ],
    )
    def test_calculate_bond_key_figure_dict(
        self,
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        result_path: str,
        isin: str,
        bond_key_figure: List[CalculatedBondKeyFigureName],
        curves: str,
        rates_shift: List[str],
        pp_speed: float,
    ) -> None:
        """Check if dictionary results are correct."""
        calc_key_figure = na_service.calculate_bond_key_figure(
            isin, bond_key_figure, anchor, pp_speed=pp_speed
        )
        bond_key_figure_names = "_".join([name.name for name in bond_key_figure])
        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    bond_key_figure_names
                    + "-"
                    + anchor.strftime("%d%m%y")
                    + "_calc_keyfigure.txt",
                ),
                "w+",
            )
            json.dump(calc_key_figure, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                "_".join([name.name for name in bond_key_figure])
                + "-"
                + anchor.strftime("%d%m%y")
                + "_calc_keyfigure.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert calc_key_figure == expected_result

    @pytest.mark.parametrize(
        "isin, bond_key_figure, curves, rates_shift, pp_speed",
        [
            (
                "DK0002000421",
                [CalculatedBondKeyFigureName.Spread, CalculatedBondKeyFigureName.Price],
                "DKKSWAP Disc OIS",
                ["0Y 5", "30Y -5"],
                0.5,
            )
        ],
    )
    def test_calculate_bond_key_figure_df(
        self,
        na_service: NordeaAnalyticsServiceTest,
        anchor: datetime,
        result_path: str,
        isin: str,
        bond_key_figure: List[CalculatedBondKeyFigureName],
        curves: str,
        rates_shift: List[str],
        pp_speed: float,
    ) -> None:
        """Check if dataframe results are correct."""
        df = na_service.calculate_bond_key_figure(
            isin, bond_key_figure, anchor, pp_speed=pp_speed, as_df=True
        )

        bond_key_figure_names = "_".join([name.name for name in bond_key_figure])
        load_and_compare_dfs(
            df,
            os.path.join(
                result_path,
                bond_key_figure_names
                + "-"
                + anchor.strftime("%d%m%y")
                + "_calc_keyfigure.csv",
            ),
            index_col=0,
            dump_data=DUMP_DATA,
            reset_index=True,
        )
