from typing import List, Union

import pytest

from nordea_analytics import NordeaAnalyticsService


class TestBondStaticData:
    """Test class for retrieving bond static data."""

    @pytest.mark.parametrize(
        "symbols",
        [
            ("DK0002030337",),
        ],
    )
    def test_bond_static_data_dict(
        self,
        na_service: NordeaAnalyticsService,
        symbols: Union[str, List[str]],
    ) -> None:
        """Check if dictionary results are correct."""
        bond_results = na_service.get_bond_static_data(
            symbols=symbols,
        )

        # .upper() cause output ISINs are also capitalised
        bond_key = (
            symbols[0][0].upper()
            if isinstance(bond_results, list)
            else symbols[0].upper()
        )
        assert bond_key in bond_results

        bond_data: dict = bond_results[bond_key]
        number_of_data_items = len(bond_data)

        assert number_of_data_items > 0

    @pytest.mark.parametrize(
        "symbols",
        [
            ("DK0002030337",),
        ],
    )
    def test_bond_static_data_df(
        self,
        na_service: NordeaAnalyticsService,
        symbols: Union[str, List[str]],
    ) -> None:
        """Check if DataFrame results are correct."""
        bond_results = na_service.get_bond_static_data(symbols=symbols, as_df=True)

        bond_key = (
            symbols[0][0].upper()
            if isinstance(bond_results, list)
            else symbols[0].upper()
        )
        assert bond_key == bond_results.index

        number_of_data_items = bond_results.values.shape[1]
        assert number_of_data_items > 0

    @pytest.mark.parametrize(
        "symbols",
        [
            ("DK0002030337",),
            (["DK0002030337"],),
        ],
    )
    def test_bond_static_data_symbols_inputs(
        self,
        na_service: NordeaAnalyticsService,
        symbols: Union[str, List[str]],
    ) -> None:
        """Check if symbols input types work as expected."""
        # to unwrap tuple
        symbols_input = symbols[0] if isinstance(symbols, tuple) else symbols

        bond_results = na_service.get_bond_static_data(
            symbols=symbols_input,
        )

        bond_key = (
            symbols_input[0].upper()
            if isinstance(symbols_input, list)
            else symbols_input.upper()
        )
        assert bond_key in bond_results

        bond_data: dict = bond_results[bond_key]
        number_of_data_items = len(bond_data)

        assert number_of_data_items > 0
