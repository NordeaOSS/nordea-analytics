"""Script for various methods for nordea analytics library."""
from abc import ABC
from enum import Enum
import json
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Union

import yaml

from nordea_analytics import (
    BondKeyFigureName,
    CalculatedBondKeyFigureName,
    CashflowType,
    CurveDefinitionName,
    CurveName,
    CurveType,
    DateRollConvention,
    DayCountConvention,
    Exchange,
    HorizonCalculatedBondKeyFigureName,
    LiveBondKeyFigureName,
    SpotForward,
    TimeConvention,
    TimeSeriesKeyFigureName,
    YieldCountry,
    YieldHorizon,
    YieldType,
)
from nordea_analytics.nalib.exceptions import AnalyticsResponseError
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
        AssetType,
        BondKeyFigureName,
        TimeSeriesKeyFigureName,
        CashflowType,
        CurveDefinitionName,
        CurveName,
        CurveType,
        DateRollConvention,
        DayCountConvention,
        Exchange,
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
) -> str:
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
        AssetType,
        BondKeyFigureName,
        TimeSeriesKeyFigureName,
        CashflowType,
        CurveDefinitionName,
        CurveName,
        CurveType,
        DateRollConvention,
        DayCountConvention,
        Exchange,
        TimeConvention,
        SpotForward,
        AmortisationType,
        CalculatedBondKeyFigureName,
        HorizonCalculatedBondKeyFigureName,
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
                # For enum types where string value is fully capitalised
                variable_type == CashflowType
                or variable_type == CurveName
                or variable_type == CurveDefinitionName
                or variable_type == CapitalCentres
                or variable_type == CapitalCentreTypes
                or variable_type == Exchange
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


class ConfigContainer(ABC):
    """Store config data."""

    config: Dict = {}


def get_config(config_path: str = None) -> Dict:
    """Find and return the config file."""
    if ConfigContainer.config and config_path is None:
        return ConfigContainer.config

    if not config_path:
        config_path = str(Path(__file__).parent / "config.yml")
    with open(config_path) as file:
        ConfigContainer.config = yaml.safe_load(file)
    return ConfigContainer.config


def float_to_tenor_string(float_tenor: Union[str, float]) -> str:
    """Convert float year fraction to tenor string."""
    if float(float_tenor).is_integer():
        return str(int(float(float_tenor))) + "Y"
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
        raise AnalyticsResponseError(
            "No data was retrieved! Please look if you have further "
            "warning messages to identify the issue."
        )


def pretty_dict_string(d: Dict, indent: int = 4, sort_keys: bool = True) -> str:
    """Print dict content as nice-formatted JSON string."""
    return json.dumps(d, indent=indent, sort_keys=sort_keys) if d else "{}"


def get_keyfigure_key(
    key_figure: str,
    key_figures_original: Union[
        List[str],
        List[CalculatedBondKeyFigureName],
        List[Union[str, CalculatedBondKeyFigureName]],
        List[HorizonCalculatedBondKeyFigureName],
        List[Union[str, HorizonCalculatedBondKeyFigureName]],
        List[LiveBondKeyFigureName],
        List[Union[str, LiveBondKeyFigureName]],
    ],
    enum_type: str,
) -> str:
    """Get keyfigure key for dict."""
    for kf_original in key_figures_original:
        if key_figure == kf_original or (
            type(key_figure) == str
            and type(kf_original) == str
            and key_figure.lower() == kf_original.lower()
        ):
            return str(kf_original)
    try:
        if enum_type == CalculatedBondKeyFigureName.__name__:
            key_figure_key = CalculatedBondKeyFigureName(key_figure).name
        elif enum_type == HorizonCalculatedBondKeyFigureName.__name__:
            key_figure_key = HorizonCalculatedBondKeyFigureName(key_figure).name
        elif enum_type == LiveBondKeyFigureName.__name__:
            key_figure_key = LiveBondKeyFigureName(key_figure).name
        else:
            raise AnalyticsResponseError(
                "Keyfigure enum type not handled explicitly, report this to package provider."
            )
    except Exception:
        key_figure_key = key_figure

    return key_figure_key


class RequestMethod(Enum):
    """Enum for request methods."""
    Get = 1
    Post = 2
