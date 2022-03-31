from pathlib import Path
from typing import Callable, Dict, List, Tuple, Union

import pytest

from nordea_analytics.curve_variable_names import CurveType, SpotForward, TimeConvention
from nordea_analytics.key_figure_names import BondKeyFigureName
from nordea_analytics.nalib.util import (
    check_json_response,
    check_string,
    convert_to_float_if_float,
    convert_to_variable_string,
    float_to_tenor_string,
    get_config,
    get_user,
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
            ["Quote", "Modduration", BondKeyFigureName.BPV, BondKeyFigureName.CVX],
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


def test_get_user_from_file() -> None:
    """Test that it is possible to read the user info from file."""
    test_path = (
        Path(__file__).parent / "data" / "expected_results" / "get_user_from_file.txt"
    )
    username = get_user(test_path)
    assert username == "test_user_name"


def test_get_user_not_exists() -> None:
    """Test that if user info is not available, return empty string."""
    test_path = (
        Path(__file__).parent / "data" / "expected_results" / "non_existing_file.txt"
    )
    username = get_user(test_path)
    assert username == ""


def test_get_config() -> None:
    """Test that config is received."""
    config = get_config()
    assert type(config) == dict


@pytest.mark.parametrize(
    "string, substring, expected_results",
    [
        ('"state":"completed", "type":', '"state":"completed"', True),
        ('"state":"processing", completed at:', '"state":"completed"', False),
    ],
)
def test_check_string(string: str, substring: str, expected_results: bool) -> None:
    """Test that returns True when exactly the right substring is given."""
    assert check_string(string, substring) == expected_results


@pytest.mark.parametrize(
    "float_tenor, expected_results", [(0.5, "6M"), ("1.5", "1.5Y"), (2.0, "2Y")]
)
def test_float_to_tenor_string(
    float_tenor: Union[str, float], expected_results: str
) -> None:
    """Test that float value is correctly returned to string tenor."""
    float_to_tenor_string(float_tenor)


@pytest.mark.parametrize("input", [([]), ({"error": {}})])
def test_check_json_response(input: Union[List, Dict]) -> None:
    """Check if the function throws an error when it receives an empty list."""
    try:
        check_json_response(input)
        expected_results = False
    except ValueError:
        expected_results = True

    assert expected_results
