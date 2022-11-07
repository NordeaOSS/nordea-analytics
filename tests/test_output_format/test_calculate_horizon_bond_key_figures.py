from datetime import datetime, timedelta
from typing import List, Union

import pytest

from nordea_analytics import (
    CurveName,
    HorizonCalculatedBondKeyFigureName,
    NordeaAnalyticsService,
)


class TestCalculateBondKeyFigure:
    """Test class for calculate key figure."""

    @pytest.mark.parametrize(
        "isin, key_figures, curves, rates_shift, pp_speed, prices",
        [
            (
                "DK0002030337",
                ["price", HorizonCalculatedBondKeyFigureName.Spread, "return_interest"],
                [
                    CurveName.DKKMTGNYK,
                    CurveName.DKKSWAP_Disc_OIS,
                    "GRDGOV",
                    "EURSWAP DISC OIS",
                ],
                ["0Y 5", "30Y -5"],
                0.5,
                None,
            ),
            (
                "DE0001102424",
                [
                    HorizonCalculatedBondKeyFigureName.PriceClean,
                    HorizonCalculatedBondKeyFigureName.ReturnInterest,
                ],
                CurveName.DEMGOV,
                None,
                None,
                100,
            ),
            (
                "DE0001102424",
                [
                    "price",
                    HorizonCalculatedBondKeyFigureName.PriceAtHorizon,
                ],
                "DKKMTGNYKSOFTBLT",
                None,
                None,
                None,
            ),
            (
                "DE0001102424",
                [
                    HorizonCalculatedBondKeyFigureName.Spread,
                    HorizonCalculatedBondKeyFigureName.PriceClean,
                    "return_interest",
                ],
                None,
                None,
                None,
                None,
            ),
        ],
    )
    def test_calculate_horizon_bond_dict(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        isin: str,
        key_figures: List[Union[str, HorizonCalculatedBondKeyFigureName]],
        curves: Union[List[str], str, CurveName, List[CurveName]],
        rates_shift: List[str],
        pp_speed: float,
        prices: Union[float, List[float]],
    ) -> None:
        """Check if dictionary results are correct."""
        calc_key_figure = na_service.calculate_horizon_bond_key_figure(
            isin,
            key_figures,
            anchor,
            horizon_date=anchor + timedelta(days=365),
            curves=curves,
            rates_shifts=rates_shift,
            pp_speed=pp_speed,
            prices=prices,
        )

        bond_results = calc_key_figure[isin]
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

        assert len(keyfigure_results) == len(key_figures)

        for kf in key_figures:
            # Enum value of keyfigure always returned, even if string is input, e.g. 'return_interest' returns ReturnInterest
            kf_enum = (
                HorizonCalculatedBondKeyFigureName(kf).name
                if isinstance(kf, HorizonCalculatedBondKeyFigureName)
                else kf
            )
            assert kf_enum in keyfigure_results

    @pytest.mark.parametrize(
        "isin, key_figures, curves, rates_shift, pp_speed",
        [
            (
                ["DK0002000421", "DK0002044551"],
                [
                    HorizonCalculatedBondKeyFigureName.Spread,
                    HorizonCalculatedBondKeyFigureName.PriceClean,
                ],
                "DKKSWAP Disc OIS",
                ["0Y 5", "30Y -5"],
                0.5,
            )
        ],
    )
    def test_calculate_horizon_bond_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        isin: List[str],
        key_figures: List[HorizonCalculatedBondKeyFigureName],
        curves: str,
        rates_shift: List[str],
        pp_speed: float,
    ) -> None:
        """Check if DataFrame results are correct."""
        bond_results = na_service.calculate_horizon_bond_key_figure(
            isin,
            key_figures,
            anchor,
            horizon_date=anchor + timedelta(days=365),
            pp_speed=pp_speed,
            as_df=True,
        )

        if curves is not None:
            _curves: list = curves if isinstance(curves, list) else [curves]
            for curve in _curves:
                curve_string = (
                    CurveName(curve).name if isinstance(curve, CurveName) else curve
                )
                assert curve_string in list(bond_results["Curve"])

        results_per_keyfigure = list(bond_results[key_figures[0].name])
        assert len(results_per_keyfigure) == len(isin)

        for kf in key_figures:
            # Enum value of keyfigure always returned, even if string is input, e.g. 'return_interest' returns ReturnInterest
            kf_enum = HorizonCalculatedBondKeyFigureName(kf).name
            assert kf_enum in bond_results

    @pytest.mark.parametrize(
        "isin, key_figures",
        [
            (
                ["DE0001102424"],
                [HorizonCalculatedBondKeyFigureName.PriceClean],
            )
        ],
    )
    def test_calculate_horizon_bond_price_clean_without_key_figures(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        isin: List[str],
        key_figures: List[HorizonCalculatedBondKeyFigureName],
    ) -> None:
        """Check if price_clean is returned when requested without other key figures."""
        bond_results = na_service.calculate_horizon_bond_key_figure(
            isin,
            key_figures,
            anchor,
            horizon_date=anchor + timedelta(days=365),
        )

        price = bond_results[isin[0]]["No curve found"][key_figures[0].name]
        assert isinstance(price, float)
