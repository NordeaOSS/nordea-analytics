from typing import Callable, List, Tuple, Union

import pytest

from nordea_analytics.bond_key_figure_name import BondKeyFigureName
from nordea_analytics.curve_variable_names import CurveType, SpotForward, TimeConvention
from nordea_analytics.nalib.util import (
    convert_to_float_if_float,
    convert_to_variable_string,
)


@pytest.mark.parametrize(
    "test_string, expected_results",
    [("string", "string"), ("string_123", "string_123")],
)
def test_convert_to_string(test_string: str, expected_results: str) -> None:
    """Test that function returns the initial string."""
    assert convert_to_float_if_float(test_string) == expected_results


@pytest.mark.parametrize("test_float, expected_results", [("123", 123), ("45", 45)])
def test_convert_to_float(test_float: str, expected_results: float) -> None:
    """Test that function converts string to float."""
    assert convert_to_float_if_float(test_float) == expected_results


@pytest.mark.parametrize(
    "test_key_figure, expected_results",
    [
        (
            ["Quote", "Modduration", BondKeyFigureName.BPVP, BondKeyFigureName.CVXP],
            ["quote", "modduration", "bpvp", "cvxp"],
        )
    ],
)
def test_keyfigure_strings(
    test_key_figure: Union[List[str], List[BondKeyFigureName]],
    expected_results: List[str],
) -> None:
    """Test that function retruns strings that service understands."""
    result = [convert_to_variable_string(t, BondKeyFigureName) for t in test_key_figure]
    assert result == expected_results


@pytest.mark.parametrize(
    "test_curve_variable_names, expected_results",
    [
        (
            [
                (CurveType.ParCurve, CurveType),
                ("Bootstrap", CurveType),
                ("act365", TimeConvention),
                (TimeConvention.TC_30EP360, TimeConvention),
                (SpotForward.Forward, SpotForward),
                ("spot", SpotForward),
                ("impliedforward", SpotForward),
            ],
            [
                "par curve",
                "bootstrap",
                "act365",
                "30ep360",
                "Forward",
                "Spot",
                "ImpliedForward",
            ],
        )
    ],
)
def test_curve_variables_name_string(
    test_curve_variable_names: List[
        Tuple[
            Union[str, CurveType, TimeConvention, SpotForward],
            Callable,
        ]
    ],
    expected_results: List[str],
) -> None:
    """Test that function returns strings that service understands."""
    result = [convert_to_variable_string(c[0], c[1]) for c in test_curve_variable_names]
    assert result == expected_results


@pytest.mark.parametrize("test_key_figure", [(["Quotes"])])
def test_keyfigure_fails(
    test_key_figure: Union[List[str], List[BondKeyFigureName]]
) -> None:
    """Test that function returns ValueError when unavailable key figure is given."""
    try:
        [convert_to_variable_string(t, BondKeyFigureName) for t in test_key_figure]
        expected_results = False
    except ValueError:
        expected_results = True
    assert expected_results
