from typing import Callable, Dict, List, Tuple, Union

import pytest

from nordea_analytics.convention_variable_names import TimeConvention
from nordea_analytics.curve_variable_names import CurveType, SpotForward
from nordea_analytics.key_figure_names import BondKeyFigureName
from nordea_analytics.nalib.exceptions import AnalyticsResponseError
from nordea_analytics.nalib.util import (
    check_json_response,
    check_json_response_error,
    convert_to_float_if_float,
    convert_to_variable_string,
    float_to_tenor_string,
    get_config,
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
def test_some_keyfigure_fails(
    test_key_figure: Union[List[str], List[BondKeyFigureName]]
) -> None:
    """Test that function returns available data despite some ISINs having no available key figures."""
    try:
        [convert_to_variable_string(t, BondKeyFigureName) for t in test_key_figure]
        expected_results = False
    except ValueError:
        expected_results = True
    assert expected_results


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
    assert (substring in string) == expected_results


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
        output_found = check_json_response(input)
        check_json_response_error(output_found)
        expected_results = False
    except AnalyticsResponseError:
        expected_results = True

    assert expected_results
