from datetime import datetime
from typing import Any, List, Union
import warnings

import pytest


from nordea_analytics import (
    CalculatedBondKeyFigureName,
    CashflowType,
    CurveName,
    NordeaAnalyticsService,
)


@pytest.fixture
def aswmm_warning() -> str:
    """Warning message returned when asw_mm calculation fails."""
    return "not supported for asset type danish mortgage bond"


class TestCalculateBondKeyFigure:
    """Test class for calculate key figure."""

    @pytest.mark.parametrize(
        "symbol, keyfigures, curves, rates_shift, pp_speed, price, spread, "
        "spread_curve, asw_fix_frequency, ladder_definition",
        [
            (
                "DK0009922320",
                [
                    "price",
                    CalculatedBondKeyFigureName.Spread,
                    CalculatedBondKeyFigureName.BPV,
                    "asw",
                ],
                [
                    CurveName.DKKMTGNYK,
                    CurveName.DKKSWAP_Disc_OIS,
                    "GRDGOV",
                    "EURSWAP DISC OIS",
                ],
                ["0Y 5", "30Y -5"],
                0.5,
                None,
                100,
                CurveName.DKKSWAP,
                None,
                None,
            ),
            (
                "DK0009922320",
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
                "DK0009922320",
                [
                    CalculatedBondKeyFigureName.PriceClean,
                    CalculatedBondKeyFigureName.Spread,
                    "asw",
                ],
                "DKKMTGNYKSOFTBLT",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "DK0009922320",
                [
                    CalculatedBondKeyFigureName.BPV,
                    CalculatedBondKeyFigureName.BPV_Ladder,
                    CalculatedBondKeyFigureName.ExpectedCashflow,
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
    def test_calculate_bond_dict(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbol: str,
        keyfigures: List[Union[str, CalculatedBondKeyFigureName]],
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
        calc_keyfigure = na_service.calculate_bond_key_figure(
            symbol,
            keyfigures,
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

        bond_results = calc_keyfigure[symbol]
        keyfigure_results: str

        if curves is not None:
            _curves: list = curves if isinstance(curves, list) else [curves]
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )
                assert curve_string in bond_results

        first_curve = list(bond_results.keys())[0]
        keyfigure_results = bond_results[first_curve]

        assert keyfigure_results.__len__() == keyfigures.__len__()

        for kf in keyfigures:
            # Enum value of keyfigure always returned, even if string is input, e.g. 'asw' returns AssetSwapSpread
            kf_enum = (
                CalculatedBondKeyFigureName(kf).name
                if isinstance(kf, CalculatedBondKeyFigureName)
                else kf
            )
            assert kf_enum in keyfigure_results

    @pytest.mark.parametrize(
        "symbols, keyfigures, curves, rates_shift, pp_speed",
        [
            (
                ["DK0009922320", "DK0009924029"],
                [
                    CalculatedBondKeyFigureName.Spread,
                    CalculatedBondKeyFigureName.PriceClean,
                ],
                ["DKKSWAP Disc OIS"],
                ["0Y 5", "30Y -5"],
                0.5,
            )
        ],
    )
    def test_calculate_bond_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: List[str],
        keyfigures: List[CalculatedBondKeyFigureName],
        curves: str,
        rates_shift: List[str],
        pp_speed: float,
    ) -> None:
        """Check if DataFrame results are correct."""
        bond_results = na_service.calculate_bond_key_figure(
            symbols, keyfigures, anchor, curves=curves, pp_speed=pp_speed, as_df=True
        )

        if curves is not None:
            _curves: list = curves if isinstance(curves, list) else [curves]
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )
                assert curve_string in list(bond_results["Curve"])

        results_per_keyfigure = list(bond_results[keyfigures[0].name])
        assert results_per_keyfigure.__len__() == symbols.__len__()

        for kf in keyfigures:
            # Enum value of keyfigure always returned, even if string is input, e.g. 'asw' returns AssetSwapSpread
            kf_enum = CalculatedBondKeyFigureName(kf).name
            assert kf_enum in bond_results

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                "DK0009922320",
                [CalculatedBondKeyFigureName.Spread],
            ),
            (
                ["DK0009922320", "DK0009924029"],
                [CalculatedBondKeyFigureName.Spread],
            ),
        ],
    )
    def test_calculate_bond_symbols_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: Union[str, List[str]],
        keyfigures: List[CalculatedBondKeyFigureName],
    ) -> None:
        """Check if symbols input types work as expected."""
        bond_results = na_service.calculate_bond_key_figure(symbols, keyfigures, anchor)

        symbols_list: list = symbols if isinstance(symbols, list) else [symbols]
        assert sorted(symbols_list) == sorted(list(bond_results.keys()))

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                ["DK0009922320"],
                CalculatedBondKeyFigureName.Spread,
            ),
            (
                ["DK0009922320"],
                "spread",
            ),
            (
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread, CalculatedBondKeyFigureName.Yield],
            ),
            (
                ["DK0009922320"],
                ["spreAD", "yield"],
            ),
            (
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread, "yield"],
            ),
        ],
    )
    def test_calculate_bond_keyfigures_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: List[str],
        keyfigures: Union[
            str,
            CalculatedBondKeyFigureName,
            List[str],
            List[CalculatedBondKeyFigureName],
            List[Union[str, CalculatedBondKeyFigureName]],
        ],
    ) -> None:
        """Check if keyfigures input types work as expected."""
        bond_results = na_service.calculate_bond_key_figure(symbols, keyfigures, anchor)

        symbol_key = symbols[0]
        curve_key = list(bond_results[symbol_key])[0]

        actual_keyfigures = list(bond_results[symbol_key][curve_key].keys())
        expected_keyfigures: list[str] = []

        _keyfigures: list = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        for kf in _keyfigures:
            expected_keyfigures.append(
                kf.name if isinstance(kf, CalculatedBondKeyFigureName) else kf
            )

        assert sorted(expected_keyfigures) == sorted(actual_keyfigures)

    @pytest.mark.parametrize(
        "symbols, keyfigures, curves",
        [
            ("DK0009922320", [CalculatedBondKeyFigureName.Spread], "DKKSWAP DISC OIS"),
            (
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread],
                ["DKKSWAP DISC OIS"],
            ),
            (
                "DK0009922320",
                [CalculatedBondKeyFigureName.Spread],
                CurveName.DKKSWAP_Disc_OIS,
            ),
            (
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread],
                [CurveName.DKKSWAP_Disc_OIS],
            ),
            (
                # Intention that 'ois' is not capitalised, output should match input exactly
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread],
                ["DKKSWAP DISC ois", CurveName.EURSWAP_Disc_OIS],
            ),
        ],
    )
    def test_calculate_bond_curves_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: Union[str, List[str]],
        keyfigures: List[CalculatedBondKeyFigureName],
        curves: Any,
    ) -> None:
        """Check if curve input types work as expected."""
        bond_results = na_service.calculate_bond_key_figure(
            symbols, keyfigures, anchor, curves=curves
        )

        bond_key = list(bond_results.keys())[0]
        expected_curves = []
        _curves: list = curves if isinstance(curves, list) else [curves]
        for curve in _curves:
            expected_curves.append(
                curve.name if isinstance(curve, CurveName) else curve
            )

        actual_curves = list(bond_results[bond_key].keys())
        assert sorted(expected_curves) == sorted(actual_curves)

    @pytest.mark.parametrize(
        "symbols, keyfigures, curves",
        [
            (
                # .name property 'DKKMTGNYK' of CurveName enum should be returned
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread],
                [CurveName.DKKMTGNYK, "GRDGOV"],
            ),
            (
                # String value 'DKKMTGNYKSOFTBLT' should be returned
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread],
                ["DKKMTGNYKSOFTBLT"],
            ),
            (
                # Curves not included in CurveName enum class should be supported
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread],
                ["GRDGOV"],
            ),
        ],
    )
    def test_calculate_bond_curves_corner_cases(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: Union[str, List[str]],
        keyfigures: List[CalculatedBondKeyFigureName],
        curves: Any,
    ) -> None:
        """Check known curves corner cases work as expected."""
        bond_results = na_service.calculate_bond_key_figure(
            symbols, keyfigures, anchor, curves=curves
        )

        bond_key = list(bond_results.keys())[0]
        expected_curves = []
        _curves: list = curves if isinstance(curves, list) else [curves]
        for curve in _curves:
            expected_curves.append(
                curve.name if isinstance(curve, CurveName) else curve
            )

        actual_curves = list(bond_results[bond_key].keys())
        assert sorted(expected_curves) == sorted(actual_curves)

    @pytest.mark.parametrize(
        "symbols, keyfigures, spread, spread_curve",
        [
            (
                # .name property 'DKKMTGNYK' of CurveName enum should be returned
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread, CalculatedBondKeyFigureName.Yield],
                100,
                "DKKSWAP DISC OIS",
            ),
            (
                # String value 'DKKMTGNYKSOFTBLT' should be returned
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread],
                100,
                CurveName.DKKSWAP_Disc_OIS,
            ),
        ],
    )
    def test_calculate_bond_spread_curve_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: Union[str, List[str]],
        keyfigures: List[CalculatedBondKeyFigureName],
        spread: float,
        spread_curve: Union[str, CurveName],
    ) -> None:
        """Check if spreac_curve input types work as expected."""
        bond_results = na_service.calculate_bond_key_figure(
            symbols, keyfigures, anchor, spread=spread, spread_curve=spread_curve
        )

        bond_key = list(bond_results.keys())[0]

        unique_keyfigures: set[str] = set().union(
            *[x.keys() for x in bond_results[bond_key].values()]
        )  # type: ignore[assignment]

        assert unique_keyfigures.__len__() == keyfigures.__len__()

        for kf in keyfigures:
            # Enum value of keyfigure always returned, even if string is input, e.g. 'asw' returns AssetSwapSpread
            kf_enum = (
                CalculatedBondKeyFigureName(kf).name
                if isinstance(kf, CalculatedBondKeyFigureName)
                else kf
            )
            assert kf_enum in unique_keyfigures

    @pytest.mark.parametrize(
        "symbols, keyfigures, cashflow_type",
        [
            (
                # .name property 'DKKMTGNYK' of CurveName enum should be returned
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread, CalculatedBondKeyFigureName.Yield],
                CashflowType.CSE,
            ),
            (
                # String value 'DKKMTGNYKSOFTBLT' should be returned
                ["DK0009922320"],
                [CalculatedBondKeyFigureName.Spread],
                "CSe",
            ),
        ],
    )
    def test_calculate_bond_cashflow_type_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: Union[str, List[str]],
        keyfigures: List[CalculatedBondKeyFigureName],
        cashflow_type: Union[str, CashflowType],
    ) -> None:
        """Check if cashflow_type input types work as expected."""
        bond_results = na_service.calculate_bond_key_figure(
            symbols, keyfigures, anchor, cashflow_type=cashflow_type
        )

        bond_key = list(bond_results.keys())[0]

        unique_keyfigures: set[str] = set().union(
            *[x.keys() for x in bond_results[bond_key].values()]
        )  # type: ignore[assignment]

        assert unique_keyfigures.__len__() == keyfigures.__len__()

        for kf in keyfigures:
            kf_enum = (
                CalculatedBondKeyFigureName(kf).name
                if isinstance(kf, CalculatedBondKeyFigureName)
                else kf
            )
            assert kf_enum in unique_keyfigures

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                ["DK0002030337"],
                [
                    CalculatedBondKeyFigureName.PriceClean,
                ],
            )
        ],
    )
    def test_calculate_bond_price_clean_without_key_figures(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: List[str],
        keyfigures: List[CalculatedBondKeyFigureName],
    ) -> None:
        """Check if price_clean is returned without other key figures being requested."""
        bond_results = na_service.calculate_bond_key_figure(symbols, keyfigures, anchor)

        price = bond_results[symbols[0]]["No curve found"][keyfigures[0].name]
        assert isinstance(price, float)

    @pytest.mark.parametrize(
        "symbols, keyfigures, asw_fix_frequency",
        [
            (
                ["DK0002030337"],
                [
                    CalculatedBondKeyFigureName.ASW_MM,
                ],
                "3M",
            )
        ],
    )
    def test_calculate_bond_failed_keyfigure_throw_warning(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: List[str],
        keyfigures: List[CalculatedBondKeyFigureName],
        asw_fix_frequency: str,
        aswmm_warning: str,
    ) -> None:
        """Check if failed key figure throw warning as expected."""
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            # Causes warning to trigger
            bond_results = na_service.calculate_bond_key_figure(  # NOQA
                symbols, keyfigures, anchor, asw_fix_frequency=asw_fix_frequency
            )

            assert len(w) == 1
            assert aswmm_warning in str(w[0].message).lower()

    @pytest.mark.parametrize(
        "symbols, keyfigures, asw_fix_frequency",
        [
            (
                ["DK0002030337"],
                [
                    CalculatedBondKeyFigureName.PriceClean,
                    CalculatedBondKeyFigureName.ASW_MM,
                ],
                "3M",
            )
        ],
    )
    def test_calculate_bond_price_clean_and_that_failed_keyfigure_throws_warning(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: List[str],
        keyfigures: List[CalculatedBondKeyFigureName],
        asw_fix_frequency: str,
        aswmm_warning: str,
    ) -> None:
        """Check if failed key figure throw warning and price_clean is still returned as expected."""
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            bond_results = na_service.calculate_bond_key_figure(
                symbols, keyfigures, anchor, asw_fix_frequency=asw_fix_frequency
            )

            price = bond_results[symbols[0]]["No curve found"][keyfigures[0].name]
            assert isinstance(price, float)
            assert len(w) == 1
            assert aswmm_warning in str(w[0].message).lower()

    @pytest.mark.parametrize(
        "symbols, keyfigures, asw_fix_frequency",
        [
            (
                ["DK0002030337"],
                [
                    CalculatedBondKeyFigureName.Spread,
                    CalculatedBondKeyFigureName.ASW_MM,
                ],
                "3M",
            )
        ],
    )
    def test_calculate_bond_working_key_figure_and_that_failed_keyfigure_throws_warning(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: List[str],
        keyfigures: List[CalculatedBondKeyFigureName],
        asw_fix_frequency: str,
        aswmm_warning: str,
    ) -> None:
        """Check if failed key figure throw warning while still returning results for other key figures as expected."""
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            bond_results = na_service.calculate_bond_key_figure(
                symbols, keyfigures, anchor, asw_fix_frequency=asw_fix_frequency
            )

            curve = list(bond_results[symbols[0]])[0]
            spread = bond_results[symbols[0]][curve][keyfigures[0].name]
            assert isinstance(spread, float)
            assert len(w) == 1
            assert aswmm_warning in str(w[0].message).lower()
