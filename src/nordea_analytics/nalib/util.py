"""Script for various methods for nordea analytics library."""
from typing import Callable, Union

from nordea_analytics.bond_key_figure_name import BondKeyFigureName
from nordea_analytics.curve_variable_names import (
    CurveType,
    SpotForward,
    TimeConvention,
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
    variable: Union[str, BondKeyFigureName, CurveType, TimeConvention, SpotForward],
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
            input not suported

    """
    if type(variable) in (BondKeyFigureName, CurveType, TimeConvention, SpotForward):
        return variable.value  # type:ignore
    elif type(variable) is str:
        try:
            if variable.lower() == "forward" or variable.lower() == "spot":
                return variable.title()
            elif variable.lower() == "impliedforward":
                return "ImpliedForward"
            else:
                variable_type(variable.lower())
            return variable.lower()
        except ValueError as e:
            raise e
    else:
        raise ValueError(str(type(variable)) + "as variable input not supported")
