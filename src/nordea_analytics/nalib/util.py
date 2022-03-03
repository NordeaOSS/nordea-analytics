"""Script for various methods for nordea analytics library."""
from pathlib import Path
from typing import Callable, Dict, Union

import yaml


from nordea_analytics.curve_variable_names import (
    CurveName,
    CurveType,
    SpotForward,
    TimeConvention,
)
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
        LiveBondKeyFigureName,
    ):
        return variable.value  # type:ignore
    elif type(variable) is str:
        try:
            if variable.lower() == "forward" or variable.lower() == "spot":
                return variable.title()
            elif variable.lower() == "impliedforward":
                return "ImpliedForward"
            elif (
                variable_type == CurveName
                or variable_type == CapitalCentres
                or variable_type == CapitalCentreTypes
            ):
                variable_type(variable.upper())
                return variable.upper()
            else:
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
    config_path = str(Path(__file__).parent / "config.yml")
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
