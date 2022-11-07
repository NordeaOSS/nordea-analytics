from datetime import datetime
from typing import List, Union
import warnings

import pandas as pd
import pytest

from nordea_analytics import (
    BondKeyFigureName,
    NordeaAnalyticsService,
)
from nordea_analytics.nalib.util import get_config

config = get_config()


class TestBondKeyFigures:
    """Test class for retrieving bond key figures."""

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                ["DK0002000421", "DK0002044551"],
                [
                    BondKeyFigureName.Spread,
                    BondKeyFigureName.PriceClean,
                ],
            )
        ],
    )
    def test_get_bond_key_figures_dict(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: Union[str, List[str]],
        keyfigures: Union[
            str,
            BondKeyFigureName,
            List[str],
            List[BondKeyFigureName],
            List[Union[str, BondKeyFigureName]],
        ],
    ) -> None:
        """Check if dictionary results are correct."""
        bond_key_figures = na_service.get_bond_key_figures(symbols, keyfigures, anchor)

        _symbols = symbols if isinstance(symbols, list) else [symbols]
        actual_bond_isins = list(bond_key_figures.keys())
        assert sorted(actual_bond_isins) == sorted(_symbols)

        _keyfigures = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        expected_bond_keyfigures: list = [
            kf.name if isinstance(kf, BondKeyFigureName) else kf for kf in _keyfigures
        ]

        for key in bond_key_figures:
            actual_bond_keyfigures = list(bond_key_figures[key].keys())
            assert sorted(actual_bond_keyfigures) == sorted(expected_bond_keyfigures)

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                ["DK0002000421", "DK0002044551"],
                [
                    BondKeyFigureName.Spread,
                    BondKeyFigureName.PriceClean,
                ],
            )
        ],
    )
    def test_get_bond_key_figures_df(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: Union[str, List[str]],
        keyfigures: Union[
            str,
            BondKeyFigureName,
            List[str],
            List[BondKeyFigureName],
            List[Union[str, BondKeyFigureName]],
        ],
    ) -> None:
        """Check if DataFrame results are correct."""
        bond_key_figures = na_service.get_bond_key_figures(
            symbols, keyfigures, anchor, as_df=True
        )

        _symbols = symbols if isinstance(symbols, list) else [symbols]
        actual_bond_isins = list(pd.DataFrame(bond_key_figures).index)
        assert sorted(actual_bond_isins) == sorted(_symbols)

        _keyfigures = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        expected_bond_keyfigures: list = [
            kf.name if isinstance(kf, BondKeyFigureName) else kf for kf in _keyfigures
        ]

        actual_bond_keyfigures = list(bond_key_figures)
        assert sorted(actual_bond_keyfigures) == sorted(expected_bond_keyfigures)

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                "DK0009408601",
                [
                    BondKeyFigureName.Spread,
                    BondKeyFigureName.PriceClean,
                ],
            ),
            (
                ["DK0009408601", "DK0002030337"],
                [
                    BondKeyFigureName.Spread,
                    BondKeyFigureName.PriceClean,
                ],
            ),
        ],
    )
    def test_get_bond_key_figures_symbols_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: Union[str, List[str]],
        keyfigures: Union[
            str,
            BondKeyFigureName,
            List[str],
            List[BondKeyFigureName],
            List[Union[str, BondKeyFigureName]],
        ],
    ) -> None:
        """Check if symbols input types work as expected."""
        bond_key_figures = na_service.get_bond_key_figures(symbols, keyfigures, anchor)

        _symbols = symbols if isinstance(symbols, list) else [symbols]
        actual_bond_isins = list(bond_key_figures.keys())
        assert sorted(actual_bond_isins) == sorted(_symbols)

        _keyfigures = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        expected_bond_keyfigures: list = [
            kf.name if isinstance(kf, BondKeyFigureName) else kf for kf in _keyfigures
        ]

        for key in bond_key_figures:
            actual_bond_keyfigures = list(bond_key_figures[key].keys())
            assert sorted(actual_bond_keyfigures) == sorted(expected_bond_keyfigures)

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                ["DK0009408601", "DK0002030337"],
                BondKeyFigureName.Spread,
            ),
            (
                ["DK0009408601", "DK0002030337"],
                [
                    BondKeyFigureName.Spread,
                    BondKeyFigureName.PriceClean,
                ],
            ),
            (
                ["DK0009408601", "DK0002030337"],
                "spreadrisk",
            ),
            (
                ["DK0009408601", "DK0002030337"],
                [
                    "spreadRISK",
                    "clean_price",
                ],
            ),
            (
                ["DK0009408601", "DK0002030337"],
                [
                    "spreadRISK",
                    BondKeyFigureName.PriceClean,
                ],
            ),
        ],
    )
    def test_get_bond_key_figures_key_figures_inputs(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        symbols: Union[str, List[str]],
        keyfigures: Union[
            str,
            BondKeyFigureName,
            List[str],
            List[BondKeyFigureName],
            List[Union[str, BondKeyFigureName]],
        ],
    ) -> None:
        """Check if key figure input types work as expected."""
        bond_key_figures = na_service.get_bond_key_figures(symbols, keyfigures, anchor)

        _symbols = symbols if isinstance(symbols, list) else [symbols]
        actual_bond_isins = list(bond_key_figures.keys())
        assert sorted(actual_bond_isins) == sorted(_symbols)

        _keyfigures = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        expected_bond_keyfigures: list = [
            kf.name if isinstance(kf, BondKeyFigureName) else kf for kf in _keyfigures
        ]

        for key in bond_key_figures:
            actual_bond_keyfigures = list(bond_key_figures[key].keys())
            assert sorted(actual_bond_keyfigures) == sorted(expected_bond_keyfigures)

    @pytest.mark.parametrize(
        "symbols_partial_spread_data, keyfigures",
        [
            (
                ["DK0009408601", "DK0002030337"],
                [
                    BondKeyFigureName.Spread,
                    BondKeyFigureName.PriceClean,
                ],
            )
        ],
    )
    def test_get_bond_key_figures_partial_data_is_returned(
        self,
        na_service: NordeaAnalyticsService,
        symbols_partial_spread_data: Union[str, List[str]],
        keyfigures: Union[
            str,
            BondKeyFigureName,
            List[str],
            List[BondKeyFigureName],
            List[Union[str, BondKeyFigureName]],
        ],
    ) -> None:
        """Check key figures are returned despite some ISINs having no key figures."""
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            anchor = datetime(2021, 7, 6)
            bond_key_figures = na_service.get_bond_key_figures(
                symbols_partial_spread_data, BondKeyFigureName.Spread, anchor
            )

            symbol_no_data = symbols_partial_spread_data[0]
            symbol_with_data = symbols_partial_spread_data[1]

            assert symbol_no_data not in bond_key_figures
            assert symbol_with_data in bond_key_figures

            assert len(w) > 0

            if isinstance(w[0].message, Warning):
                warning_message = w[0].message.args[0]
                assert (
                    "DK0009408601" and "topic" and "does not exist" in warning_message
                )
            else:
                raise Exception("Warning expected")

    @pytest.mark.parametrize(
        "symbols_partial_spread_data, keyfigures",
        [
            (
                ["DK0009408601"],
                [
                    BondKeyFigureName.Spread,
                    BondKeyFigureName.PriceClean,
                ],
            )
        ],
    )
    def test_get_bond_key_figures_no_data_is_returned(
        self,
        na_service: NordeaAnalyticsService,
        symbols_partial_spread_data: Union[str, List[str]],
        keyfigures: Union[
            str,
            BondKeyFigureName,
            List[str],
            List[BondKeyFigureName],
            List[Union[str, BondKeyFigureName]],
        ],
    ) -> None:
        """Check empty dictionary is returned when no data is available."""
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            anchor = datetime(2021, 7, 4)
            bond_key_figures = na_service.get_bond_key_figures(
                symbols_partial_spread_data, BondKeyFigureName.Spread, anchor
            )

            assert len(bond_key_figures) == 0

            assert len(w) > 0

            if isinstance(w[0].message, Warning):
                warning_message = w[0].message.args[0]
                assert (
                    "DK0009408601" and "topic" and "does not exist" in warning_message
                )
            else:
                raise Exception("Warning expected")

    @pytest.mark.parametrize(
        "symbols, keyfigures",
        [
            (
                [
                    "US91282CCN92",
                    "US912796U492",
                    "US912828YF19",
                    "XS0320606840",
                    "US912796U567",
                    "US912796U641",
                    "US9128282W90",
                    "US912828L575",
                    "US91282CAN11",
                    "US912796M895",
                    "US912796V557",
                    "US912828YK04",
                    "US912796V631",
                    "US912796V714",
                    "US9128283C28",
                    "US912828M490",
                    "US91282CAR25",
                    "US912796N968",
                    "US912796W548",
                    "US912810EN47",
                    "US912828TY62",
                    "US912796W621",
                    "US912796W704",
                    "US912828M805",
                    "US91282CAX92",
                    "US912796P948",
                    "US912796X611",
                    "US912796X793",
                    "US912828YW42",
                    "US912796X876",
                    "XS2160873480",
                    "US912796R274",
                    "US912828N308",
                    "US91282CBD20",
                    "US912796X959",
                    "US912796XR55",
                    "US912828UH11",
                    "US912828Z294",
                    "US912796XS39",
                    "US912796S348",
                    "US9128283U26",
                    "US912828P386",
                    "US91282CBG50",
                    "US912796XT12",
                    "US912810EP94",
                    "US912828UN88",
                    "US912828Z864",
                    "US912796YA12",
                    "US912796T338",
                    "US9128284A52",
                    "US912828P790",
                    "US91282CBN02",
                    "US912796YB94",
                    "US912796YK93",
                    "US912828ZD51",
                    "XS1791714147",
                    "US912796U310",
                    "US9128284D91",
                    "US912828Q293",
                    "US91282CBU45",
                    "US9128284H06",
                    "US912828ZH65",
                    "US912796V482",
                    "US9128284L18",
                    "US912828R283",
                    "US91282CBX83",
                    "US912828VB32",
                    "US912828ZP81",
                    "XS1779710901",
                    "US912796W472",
                    "US9128284S60",
                    "US912828R697",
                    "US91282CCD11",
                    "US912796X538",
                    "US912828ZU76",
                    "US9128284U17",
                    "US912828S356",
                    "US91282CCK53",
                    "US912796XQ72",
                    "US912828VM96",
                    "US912828ZY98",
                    "XS1856338022",
                    "US912828S927",
                    "US912828Y610",
                ],
                [
                    BondKeyFigureName.Spread,
                    BondKeyFigureName.PriceClean,
                ],
            )
        ],
    )
    def test_get_bond_key_figures_symbols_exceeding_bond_limit_should_succeed(
        self,
        na_service: NordeaAnalyticsService,
        symbols: Union[str, List[str]],
        keyfigures: Union[
            str,
            BondKeyFigureName,
            List[str],
            List[BondKeyFigureName],
            List[Union[str, BondKeyFigureName]],
        ],
    ) -> None:
        """Check if result for all bonds is returned despite being over the symbol limit."""
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            max_bonds = config["max_bonds"]
            anchor = datetime(2021, 7, 6)
            bond_key_figures = na_service.get_bond_key_figures(
                symbols, BondKeyFigureName.Spread, anchor
            )

            assert len(bond_key_figures) > max_bonds
            assert len(w) > 0

            if isinstance(w[0].message, Warning):
                warning_message = w[0].message.args[0]
                assert (
                    "US91282CCN92" and "topic" and "does not exist" in warning_message
                )
            else:
                raise Exception("Warning expected")
