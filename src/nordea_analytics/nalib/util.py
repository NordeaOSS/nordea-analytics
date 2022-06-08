"""Script for various methods for nordea analytics library."""
import os
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Union

import yaml

from nordea_analytics.curve_variable_names import (
    CurveDefinitionName,
    CurveName,
    CurveType,
    SpotForward,
    TimeConvention,
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
)


def convert_to_float_if_float(string: str) -> Union[str, float]:
    """Converts a given string to a float, if the string has a float format.

    Args:
        string: string that should maybe be converted.

    Returns:
         float value if possible, else the given string.
    """
    try:
        return_value = float(string)
        return return_value
    except ValueError:
        return string


def convert_to_variable_string(
    variable: Union[
        str,
        BondKeyFigureName,
        TimeSeriesKeyFigureName,
        CurveDefinitionName,
        CurveName,
        CurveType,
        TimeConvention,
        SpotForward,
        AmortisationType,
        CalculatedBondKeyFigureName,
        HorizonCalculatedBondKeyFigureName,
        AssetType,
        CapitalCentres,
        CapitalCentreTypes,
        YieldCountry,
        YieldType,
        YieldHorizon,
        LiveBondKeyFigureName,
    ],
    variable_type: Callable,
) -> Union[str, ValueError]:
    """Convert of any variable name to string which is available in the service.

    Args:
        variable: variable which should be converted
            and/or checked. Can be any of the variable
            names or string.
        variable_type: Called to check if string available in service.

    Returns:
        Variable name as a string that can be retrieved from the service.

    Raises:
        ValueError: If string value is not valid for service or variable
            input not supported

    """
    if type(variable) in (
        BondKeyFigureName,
        TimeSeriesKeyFigureName,
        CurveDefinitionName,
        CurveName,
        CurveType,
        TimeConvention,
        SpotForward,
        AmortisationType,
        CalculatedBondKeyFigureName,
        HorizonCalculatedBondKeyFigureName,
        AssetType,
        CapitalCentres,
        CapitalCentreTypes,
        YieldCountry,
        YieldType,
        YieldHorizon,
        LiveBondKeyFigureName,
    ):
        try:
            variable_type(variable.value)  # type:ignore
            return variable.value  # type:ignore
        except ValueError as e:
            raise e
    elif type(variable) is str:
        try:
            if variable.lower() == "forward" or variable.lower() == "spot":
                return variable.title()
            elif variable.lower() == "impliedforward":
                return "ImpliedForward"
            elif (
                variable_type == CurveName
                or variable_type == CurveDefinitionName
                or variable_type == CapitalCentres
                or variable_type == CapitalCentreTypes
                or variable_type == YieldCountry
                or variable_type == YieldHorizon
            ):
                variable_type(variable.upper())
                return variable.upper()
            else:
                # to cause ValueError when incorrect key figure is passed, e.g. "Quotes"
                variable_type(variable.lower())
                return variable.lower()
        except ValueError as e:
            raise e
    else:
        raise ValueError(str(type(variable)) + "as variable input not supported")


def get_user(user_path: Path) -> str:
    """Get user information from .user_info if available.

    Args:
        user_path: Path where .user_info should be saved.

    Returns:
        User info in form of string if available, else empty string.
    """
    if user_path.exists():
        user_info = user_path.read_text()
        return user_info
    else:
        return ""


def get_config() -> Dict:
    """Find and return the config file."""
    config_file_name = os.environ.get("ANALYTICS_API_CONFIG", "config.yml")
    config_path = str(Path(__file__).parent / config_file_name)
    with open(config_path) as file:
        config = yaml.safe_load(file)
    return config


def get_config_test() -> Dict:
    """Find and return the config file."""
    config_file_name = "config_test.yml"
    config_path = str(Path(__file__).parent / config_file_name)
    with open(config_path) as file:
        config = yaml.safe_load(file)
    return config


def check_string(string: str, substring: str) -> bool:
    """Strictly checks if substring is in string."""
    try:
        string.index(substring)
    except ValueError:
        return False
    else:
        return True


def float_to_tenor_string(float_tenor: Union[str, float]) -> str:
    """Convert float year fraction to tenor string."""
    if float(float_tenor).is_integer():
        return str(int(float_tenor)) + "Y"
    else:
        return str(float(float_tenor)) + "Y"


def check_json_response(json_response: Union[List, Mapping]) -> bool:
    """Check if json_response is empty and returns False, else True."""
    if not json_response or (
        type(json_response) == dict and all(not json_response[d] for d in json_response)
    ):
        return False
    else:
        return True


def check_json_response_error(output_found: bool) -> None:
    """Throws error if output in json_response isn't found."""
    if not output_found:
        raise ValueError(
            "No data was retrieved! Please look if you have further "
            "warning messages to identify the issue."
        )
