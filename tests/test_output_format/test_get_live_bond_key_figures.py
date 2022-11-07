from typing import Any, List, Union

import pytest

from nordea_analytics import (
    LiveBondKeyFigureName,
    NordeaAnalyticsService,
)


class TestLiveBondKeyFigures:
    """Test class for retrieving live bond key figures."""

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                "DK0002050871",
                LiveBondKeyFigureName.LiborSpread3M,
            ),
        ],
    )
    def test_live_bond_key_figures_dict(
        self,
        na_service: NordeaAnalyticsService,
        symbols: Union[str, List[str]],
        keyfigures: Any,
    ) -> None:
        """Check if dictionary results are correct."""
        bond_results = na_service.get_bond_live_key_figures(
            symbols=symbols,
            keyfigures=keyfigures,
        )

        _symbols = symbols if isinstance(symbols, list) else [symbols]
        _keyfigures = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        for symbol in _symbols:
            assert symbol in bond_results

            assert "timestamp" in bond_results[symbol]

            for kf in _keyfigures:
                kf_string = kf.name if isinstance(kf, LiveBondKeyFigureName) else kf
                assert kf_string in bond_results[symbol]

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                "DK0002050871",
                LiveBondKeyFigureName.LiborSpread3M,
            ),
        ],
    )
    def test_live_bond_key_figures_df(
        self,
        na_service: NordeaAnalyticsService,
        symbols: Union[str, List[str]],
        keyfigures: Any,
    ) -> None:
        """Check if DataFrame results are correct."""
        bond_results = na_service.get_bond_live_key_figures(
            symbols=symbols, keyfigures=keyfigures, as_df=True
        )

        _symbols = symbols if isinstance(symbols, list) else [symbols]
        _keyfigures = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        for symbol in _symbols:
            assert symbol in list(bond_results.ISIN)

            assert "timestamp" in bond_results

            for kf in _keyfigures:
                kf_string = kf.name if isinstance(kf, LiveBondKeyFigureName) else kf
                assert kf_string in bond_results

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                "DK0002050871",
                LiveBondKeyFigureName.LiborSpread3M,
            ),
            (
                ["DK0002050871", "DK0009918138"],
                LiveBondKeyFigureName.LiborSpread3M,
            ),
        ],
    )
    def test_live_bond_symbols_inputs(
        self,
        na_service: NordeaAnalyticsService,
        symbols: Union[str, List[str]],
        keyfigures: Any,
    ) -> None:
        """Check if symbols input types work as expected."""
        bond_results = na_service.get_bond_live_key_figures(
            symbols=symbols,
            keyfigures=keyfigures,
        )

        _symbols = symbols if isinstance(symbols, list) else [symbols]
        _keyfigures = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        for symbol in _symbols:
            assert symbol in bond_results

            assert "timestamp" in bond_results[symbol]

            for kf in _keyfigures:
                kf_string = kf.name if isinstance(kf, LiveBondKeyFigureName) else kf
                assert kf_string in bond_results[symbol]

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                "DK0002050871",
                LiveBondKeyFigureName.LiborSpread6M,
            ),
            (
                "DK0002050871",
                "libor 6m spread",
            ),
            (
                "DK0002050871",
                [LiveBondKeyFigureName.LiborSpread6M, LiveBondKeyFigureName.GovSpread],
            ),
            (
                "DK0002050871",
                ["libor 6m spread", "gov SPREAD"],
            ),
            (
                "DK0002050871",
                [LiveBondKeyFigureName.LiborSpread6M, "gov spread"],
            ),
        ],
    )
    def test_live_bond_keyfigures_inputs(
        self,
        na_service: NordeaAnalyticsService,
        symbols: Union[str, List[str]],
        keyfigures: Any,
    ) -> None:
        """Check if symbols input types work as expected."""
        bond_results = na_service.get_bond_live_key_figures(
            symbols=symbols,
            keyfigures=keyfigures,
        )

        _symbols = symbols if isinstance(symbols, list) else [symbols]
        _keyfigures = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        for symbol in _symbols:
            assert symbol in bond_results

            assert "timestamp" in bond_results[symbol]

            for kf in _keyfigures:
                kf_string = kf.name if isinstance(kf, LiveBondKeyFigureName) else kf
                assert kf_string in bond_results[symbol]
