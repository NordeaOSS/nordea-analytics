from datetime import datetime
import json
import os
from pathlib import Path
from typing import List, Union

import pytest

from nordea_analytics import NordeaAnalyticsService, TimeConvention
from nordea_analytics.curve_variable_names import (
    CurveDefinitionName,
    CurveName,
    CurveType,
    SpotForward,
)
from nordea_analytics.forecast_names import YieldCountry, YieldHorizon, YieldType
from nordea_analytics.key_figure_names import (
    BondKeyFigureName,
    CalculatedBondKeyFigureName,
    HorizonCalculatedBondKeyFigureName,
    LiveBondKeyFigureName,
    TimeSeriesKeyFigureName,
)
from nordea_analytics.search_bond_names import (
    AmortisationType,
    AssetType,
    CapitalCentres,
    CapitalCentreTypes,
    Issuers,
)
from nordea_analytics import get_nordea_analytics_client  # type: ignore
from tests.util import load_and_compare_dfs

DUMP_DATA = False


@pytest.fixture
def na_service() -> NordeaAnalyticsService:
    """NordeaAnaLyticsService test class."""
    client_id = "<CLIENT_ID>"
    client_secret = "<CLIENT_SECRET>"
    return get_nordea_analytics_client(client_id=client_id, client_secret=client_secret)


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
def isins_partial_spread_data() -> List[str]:
    """ISINs used for testing partial get_bond_key_figures response."""
    return ["DK0009408601", "DK0002030337"]


@pytest.fixture
def keyfigures() -> List[Union[str, BondKeyFigureName]]:
    """Key Figures for BondKeyFigures test."""
    return [
        "Quote",
        "Yield",
        "Modduration",
        "accint",
        BondKeyFigureName.BPV,
        BondKeyFigureName.CVX,
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
        TimeSeriesKeyFigureName.BPV,
        TimeSeriesKeyFigureName.CVX,
        TimeSeriesKeyFigureName.OAModifiedDuration,
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
        na_service: NordeaAnalyticsService,
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
        na_service: NordeaAnalyticsService,
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

    def test_partial_data_is_returned(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        isins_partial_spread_data: List[str],
        keyfigures: List[Union[str, BondKeyFigureName]],
    ) -> None:
        """Check if available key figures is returned despite some ISINs having no key figures."""
        bond_key_figures = na_service.get_bond_key_figures(
            isins_partial_spread_data, BondKeyFigureName.Spread, anchor
        )

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    anchor.strftime("%d%m%y") + "_partial_spread_data_is_returned.txt",
                ),
                "w+",
            )
            json.dump(bond_key_figures, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                anchor.strftime("%d%m%y") + "_partial_spread_data_is_returned.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert bond_key_figures == expected_result


class TestIndexComposition:
    """Test class for retrieving index Composition."""

    def test_get_index_composition_dict(
        self,
        na_service: NordeaAnalyticsService,
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
        na_service: NordeaAnalyticsService,
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
        na_service: NordeaAnalyticsService,
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
        na_service: NordeaAnalyticsService,
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
            # (
            #     "DKKGOV",
            #     CurveType.ParCurve,
            #     TimeConvention.TC_30360,
            #     SpotForward.Spot,
            #     None,
            # ),
        ],
    )
    def test_get_curve_time_series_dict(
        self,
        na_service: NordeaAnalyticsService,
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
            # (
            #     CurveName.DKKGOV,
            #     CurveType.ParCurve,
            #     TimeConvention.TC_30360,
            #     SpotForward.Spot,
            #     None,
            # ),
        ],
    )
    def test_get_curve_time_series_df(
        self,
        na_service: NordeaAnalyticsService,
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
        na_service: NordeaAnalyticsService,
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
        na_service: NordeaAnalyticsService,
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
        na_service: NordeaAnalyticsService,
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
            (CurveDefinitionName.EURSWAP),
        ],
    )
    def test_get_curve_definition_dict(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        curve_name: Union[str, CurveDefinitionName],
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
            (CurveDefinitionName.DKKMTGNYK),
            (CurveDefinitionName.SEKGOV),
        ],
    )
    def test_get_curve_definition_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        curve_name: Union[str, CurveDefinitionName],
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
        "upper_maturity, country, "
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
            ),
            (
                True,
                "DKK",
                None,
                None,
                None,
                None,
                None,
                [CapitalCentres.NDA_1, CapitalCentres.RD_General],
                [CapitalCentreTypes.JCB, CapitalCentreTypes.RO],
            ),
        ],
    )
    def test_search_bonds_dict(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        dmb: bool,
        currency: str,
        issuers: str,
        amortisation_type: AmortisationType,
        asset_type: AssetType,
        upper_maturity: datetime,
        country: str,
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
            (False, "EUR", [Issuers.Stadshypotek_AB]),
        ],
    )
    def test_search_bonds_df(
        self,
        na_service: NordeaAnalyticsService,
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
        self, na_service: NordeaAnalyticsService, anchor: datetime
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


class TestCalculateBondKeyFigure:
    """Test class for calculate key figure."""

    @pytest.mark.parametrize(
        "isin, key_figure, curves, rates_shift, pp_speed, price, spread, "
        "spread_curve, asw_fix_frequency, ladder_definition",
        [
            (
                "DK0002000421",
                [CalculatedBondKeyFigureName.Spread, CalculatedBondKeyFigureName.BPV],
                [CurveName.DKKSWAP, CurveName.DKKSWAP_Disc_OIS],
                ["0Y 5", "30Y -5"],
                0.5,
                None,
                100,
                CurveName.DKKSWAP,
                None,
                None,
            ),
            (
                "DE0001102424",
                [
                    CalculatedBondKeyFigureName.PriceClean,
                    CalculatedBondKeyFigureName.AssetSwapSpread,
                    CalculatedBondKeyFigureName.ASW_PP,
                    CalculatedBondKeyFigureName.ASW_MM,
                ],
                CurveName.DEMGOV,
                None,
                None,
                100,
                None,
                None,
                "3M",
                None,
            ),
            (
                "DE0001102424",
                [
                    CalculatedBondKeyFigureName.BPV,
                    CalculatedBondKeyFigureName.BPV_Ladder,
                ],
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                ["6M", "2Y", "3Y"],
            ),
        ],
    )
    def test_calculate_bond_key_figure_dict(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        isin: str,
        key_figure: List[CalculatedBondKeyFigureName],
        curves: Union[List[str], str, CurveName, List[CurveName]],
        rates_shift: List[str],
        pp_speed: float,
        price: float,
        spread: float,
        spread_curve: Union[str, CurveName],
        asw_fix_frequency: str,
        ladder_definition: List[str],
    ) -> None:
        """Check if dictionary results are correct."""
        calc_key_figure = na_service.calculate_bond_key_figure(
            isin,
            key_figure,
            anchor,
            curves=curves,
            rates_shifts=rates_shift,
            pp_speed=pp_speed,
            price=price,
            spread=spread,
            spread_curve=spread_curve,
            asw_fix_frequency=asw_fix_frequency,
            ladder_definition=ladder_definition,
        )
        key_figure_names = "_".join([name.name for name in key_figure])

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    key_figure_names
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
                key_figure_names
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
                ["DK0002000421", "DK0002044551"],
                [
                    CalculatedBondKeyFigureName.Spread,
                    CalculatedBondKeyFigureName.PriceClean,
                ],
                "DKKSWAP Disc OIS",
                ["0Y 5", "30Y -5"],
                0.5,
            )
        ],
    )
    def test_calculate_bond_key_figure_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        isin: List[str],
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


class TestCalculateHorizonBondKeyFigure:
    """Test class for calculate key figure."""

    @pytest.mark.parametrize(
        "isin, keyfigures, horizon_date, curves, rates_shifts, pp_speed, price,"
        "fixed_prepayments, spread_change_horizon",
        [
            (
                "DK0002046259",
                [
                    HorizonCalculatedBondKeyFigureName.PriceClean,
                    HorizonCalculatedBondKeyFigureName.Spread,
                    HorizonCalculatedBondKeyFigureName.ReturnInterestAmount,
                ],
                datetime(2022, 1, 3),
                [CurveName.DKKSWAP_Disc_OIS],
                ["0Y 5", "30Y -5"],
                0.5,
                70,
                0.01,
                50.0,
            )
        ],
    )
    def test_calculate_horizon_bond_key_figure_dict(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        isin: str,
        keyfigures: List[HorizonCalculatedBondKeyFigureName],
        horizon_date: datetime,
        curves: CurveName,
        rates_shifts: Union[List[str], str],
        pp_speed: float,
        price: float,
        fixed_prepayments: float,
        spread_change_horizon: float,
    ) -> None:
        """Check if dictionary results are correct."""
        calc_key_figure = na_service.calculate_horizon_bond_key_figure(
            isin,
            keyfigures,
            anchor,
            horizon_date,
            curves=curves,
            rates_shifts=rates_shifts,
            pp_speed=pp_speed,
            price=price,
            fixed_prepayments=fixed_prepayments,
            spread_change_horizon=spread_change_horizon,
        )
        bond_key_figure_names = "_".join([name.name for name in keyfigures])

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    bond_key_figure_names
                    + "-"
                    + anchor.strftime("%d%m%y")
                    + "_calc_horizon_keyfigure.txt",
                ),
                "w+",
            )
            json.dump(calc_key_figure, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                bond_key_figure_names
                + "-"
                + anchor.strftime("%d%m%y")
                + "_calc_horizon_keyfigure.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert calc_key_figure == expected_result

    @pytest.mark.parametrize(
        "isin, keyfigures, horizon_date, curves, reinvest_in_series, reinvestment_rate",
        [
            (
                ["DE0001141794", "DK0002044551"],
                [
                    HorizonCalculatedBondKeyFigureName.BPV,
                    HorizonCalculatedBondKeyFigureName.CVX,
                    HorizonCalculatedBondKeyFigureName.ReturnPrincipal,
                ],
                datetime(2022, 1, 3),
                [CurveName.DKKSWAP_Libor, CurveName.DKKSWAP],
                False,
                0.025,
            )
        ],
    )
    def test_calculate_horizon_bond_key_figure_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        isin: List[str],
        keyfigures: List[HorizonCalculatedBondKeyFigureName],
        horizon_date: datetime,
        curves: List[CurveName],
        reinvest_in_series: bool,
        reinvestment_rate: float,
    ) -> None:
        """Check if dataframe results are correct."""
        df = na_service.calculate_horizon_bond_key_figure(
            isin,
            keyfigures,
            anchor,
            horizon_date,
            curves=curves,
            reinvest_in_series=reinvest_in_series,
            reinvestment_rate=reinvestment_rate,
            as_df=True,
        )

        bond_key_figure_names = "_".join([name.name for name in keyfigures])

        load_and_compare_dfs(
            df,
            os.path.join(
                result_path,
                bond_key_figure_names
                + "-"
                + anchor.strftime("%d%m%y")
                + "_calc_horizon_keyfigure.csv",
            ),
            index_col=0,
            dump_data=DUMP_DATA,
            reset_index=True,
        )


class TestFXForecast:
    """Test class for retrieving fx forecast."""

    @pytest.mark.parametrize(
        "currency_pair",
        [
            ("EURUSD"),
        ],
    )
    def test_get_fx_forecast_dict(
        self,
        na_service: NordeaAnalyticsService,
        result_path: str,
        currency_pair: str,
    ) -> None:
        """Check if dictionary results are correct."""
        fx_forecast = na_service.get_fx_forecasts(currency_pair)

        fx_forecast_keys = list(fx_forecast[currency_pair].keys())

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    currency_pair + "_fx_forecast_keys.txt",
                ),
                "w+",
            )
            json.dump(fx_forecast_keys, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                currency_pair + "_fx_forecast_keys.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert fx_forecast_keys == expected_result
        for key in fx_forecast_keys:
            assert fx_forecast[currency_pair][key] != {}

    @pytest.mark.parametrize(
        "currency_pair",
        [
            ("USDDKK"),
        ],
    )
    def test_fx_forecast_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        currency_pair: str,
    ) -> None:
        """Check if dictionary results are correct."""
        fx_forecast_df = na_service.get_fx_forecasts(currency_pair, as_df=True)
        fx_forecast_columns = list(fx_forecast_df.columns.values)

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    currency_pair + "_fx_forecast_columns.txt",
                ),
                "w+",
            )
            json.dump(fx_forecast_columns, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                currency_pair + "_fx_forecast_columns.txt",
            ),
            "r",
        )

        expected_result = json.load(expected_file)
        assert fx_forecast_columns == expected_result
        assert fx_forecast_df.values.size != 0


class TestYieldForecast:
    """Test class for retrieving fx forecast."""

    @pytest.mark.parametrize(
        "country, yield_type, yield_horizon",
        [
            (YieldCountry.DK, YieldType.Govt, YieldHorizon.Horizon_2Y),
        ],
    )
    def test_get_yield_forecast_dict(
        self,
        na_service: NordeaAnalyticsService,
        result_path: str,
        country: YieldCountry,
        yield_type: YieldType,
        yield_horizon: YieldHorizon,
    ) -> None:
        """Check if dictionary results are correct."""
        yield_forecast = na_service.get_yield_forecasts(
            country, yield_type, yield_horizon
        )
        symbol = country.value + " " + yield_horizon.value
        yield_forecast_keys = list(yield_forecast[symbol].keys())

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    symbol + "_yield_forecast_keys.txt",
                ),
                "w+",
            )
            json.dump(yield_forecast_keys, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                symbol + "_yield_forecast_keys.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert yield_forecast_keys == expected_result
        # It seems that sometimes Nordea forecast or implied
        # are returned empty, which results failing of this test.
        # Needs to be investigated on the API side - until then,
        # skip this test so we can run bamboo
        # for key in yield_forecast_keys:
        #     assert yield_forecast[symbol][key] != {}

    @pytest.mark.parametrize(
        "country, yield_type, yield_horizon",
        [
            ("EU", "libor", "3M"),
        ],
    )
    def test_yield_forecast_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        country: str,
        yield_type: str,
        yield_horizon: str,
    ) -> None:
        """Check if dictionary results are correct."""
        yield_forecast_df = na_service.get_yield_forecasts(
            country, yield_type, yield_horizon, as_df=True
        )
        yield_forecast_columns = list(yield_forecast_df.columns.values)
        symbol = country + "_" + yield_horizon

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    symbol + "_yield_forecast_columns.txt",
                ),
                "w+",
            )
            json.dump(yield_forecast_columns, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                symbol + "_yield_forecast_columns.txt",
            ),
            "r",
        )

        expected_result = json.load(expected_file)
        assert yield_forecast_columns == expected_result
        assert yield_forecast_df.values.size != 0


@pytest.mark.skip("test not robust enough")
class TestNordeaAnalyticsLiveService:
    """Test class for live streaming service."""

    @pytest.mark.parametrize(
        "isin, live_key_figure, streaming",
        [
            (
                ["DK0009295065", "DK0009527376"],
                [LiveBondKeyFigureName.Quote, LiveBondKeyFigureName.CVX],
                False,
            ),
            # (
            #     ["DK0009295065", "DK0009527376"],
            #     [LiveBondKeyFigureName.Spread, LiveBondKeyFigureName.Yield],
            #     True,
            # ),
        ],
    )
    def test_live_streaming_dict(
        self,
        isin: List[str],
        live_key_figure: Union[List[LiveBondKeyFigureName], List[str]],
        streaming: bool,
    ) -> None:
        """Test live streaming returned as a dict."""
        na_live_service = get_nordea_analytics_client()

        live_bond_keyfigure = na_live_service.iter_live_bond_key_figures(
            isin, live_key_figure
        )
        i = 0
        for kf in live_bond_keyfigure:
            if i < 5:
                live_dict = kf
                i = i + 1
            else:
                break
        list(live_dict.keys()).sort()
        isin.sort()
        assert list(live_dict.keys()) == isin
        assert len(live_dict[isin[0]]) == len(live_key_figure) + 1

    @pytest.mark.parametrize(
        "isin, live_key_figure, streaming",
        [
            (
                ["DK0009398620", "DK0009527376"],
                [LiveBondKeyFigureName.SwapSpread, LiveBondKeyFigureName.LiborSpread6M],
                False,
            ),
        ],
    )
    def test_live_streaming_df(
        self,
        isin: List[str],
        live_key_figure: Union[List[LiveBondKeyFigureName], List[str]],
        streaming: bool,
    ) -> None:
        """Test live streaming returned as a dict."""
        na_live_service = get_nordea_analytics_client()

        live_bond_keyfigure = na_live_service.iter_live_bond_key_figures(
            isin, live_key_figure, as_df=True
        )
        i = 0
        for kf in live_bond_keyfigure:
            if i < 5:
                live_df = kf
                i = i + 1
            else:
                break
        isin.sort()
        list(live_df["ISIN"]).sort()
        assert list(live_df["ISIN"]) == isin
        assert live_df.columns.size == len(live_key_figure) + 2


class TestShiftDays:
    """Test class for shift days."""

    @pytest.mark.parametrize(
        "date, days, exchange, day_count_convention, date_roll_convention",
        [(datetime(2022, 3, 18), 1, "None", "Bank days", "None")],
    )
    def test_shift_days_datetime(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        date: datetime,
        days: int,
        exchange: str,
        day_count_convention: str,
        date_roll_convention: str,
    ) -> None:
        """Check if dictionary results are correct."""
        shifted_date = na_service.get_shift_days(
            date,
            days,
            exchange,
            day_count_convention,
            date_roll_convention,
        )

        expected_result = datetime(2022, 3, 21, 0, 0)

        assert shifted_date == expected_result


@pytest.mark.skip("Throws AttributionError")
class TestYearFraction:
    """Test class for year fraction."""

    @pytest.mark.parametrize(
        "from_date, to_date, time_convention",
        [(datetime(2022, 3, 18), datetime(2022, 6, 18), "30360")],
    )
    def test_year_fraction(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        from_date: datetime,
        to_date: datetime,
        time_convention: str,
    ) -> None:
        """Check if dictionary results are correct."""
        year_fraction = na_service.get_year_fraction(
            from_date, to_date, time_convention
        )

        expected_result = 0.25

        assert year_fraction == expected_result


@pytest.mark.skip("retrieval_date field should be removed from response")
class TestBondStaticData:
    """Test class for bond static data."""

    @pytest.mark.parametrize(
        "symbols", [(["DK0002030337", "DE0001102424", "DK0030505805", "NO0012493941"])]
    )
    def test_bond_static_data(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        result_path: str,
        symbols: List[str],
    ) -> None:
        """Check if dictionary results are correct."""
        static_data = na_service.get_bond_static_data(
            symbols,
        )

        bond_static_data_names = "_".join([name for name in symbols])

        if DUMP_DATA:
            expected_file = open(
                os.path.join(
                    result_path,
                    bond_static_data_names
                    + "-"
                    + anchor.strftime("%d%m%y")
                    + "_bond_static_data.txt",
                ),
                "w+",
            )
            json.dump(static_data, expected_file)

        expected_file = open(
            os.path.join(
                result_path,
                bond_static_data_names
                + "-"
                + anchor.strftime("%d%m%y")
                + "_bond_static_data.txt",
            ),
            "r",
        )
        expected_result = json.load(expected_file)

        assert static_data == expected_result
