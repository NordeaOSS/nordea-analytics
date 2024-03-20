from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from nordea_analytics.convention_variable_names import TimeConvention
from nordea_analytics.curve_variable_names import (
    CurveName,
    CurveType,
    SpotForward,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.exceptions import AnalyticsInputError, CustomWarningCheck
from nordea_analytics.nalib.util import (
    convert_to_float_if_float,
    convert_to_original_format,
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class Curve(ValueRetriever):
    """Retrieves and reformats curves.

    This class provides methods for retrieving and reformatting curves from a
    data retrieval service. It inherits from the `ValueRetriever` class.

    Args:
        client (DataRetrievalServiceClient):
            An instance of the DataRetrievalServiceClient class for making
            requests to the data retrieval service.
        curves (str, CurveName, List[str], List[CurveName], List[Union[str, CurveName]]):
            Name of the curves that should be retrieved. Can be a single curve name
            as a string, a CurveName enum value, a list of curve names as strings,
            a list of CurveName enum values, or a list of strings or CurveName enum
            values mixed together.
        calc_date (datetime):
            The calculation date for the request.
        curve_type (str, CurveType, optional):
            The type of curve to be retrieved. Default is None.
        tenor_frequency (float, optional):
            The frequency between tenors as a fraction of a year. Default is None.
        time_convention (str, TimeConvention, optional):
            The time convention used when the curve is constructed. Default is None.
        spot_forward (str, SpotForward, optional):
            The type of curve to retrieve - spot, spot forward, or implied forward.
            Default is None.
        forward_tenor (float, optional):
            The forward tenor for a forward curve or implied forward curve. Default is None.
    """

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        curves: Union[
            str,
            CurveName,
            List[str],
            List[CurveName],
            List[Union[str, CurveName]],
        ],
        calc_date: datetime,
        curve_type: Optional[Union[str, CurveType]] = None,
        tenor_frequency: Optional[float] = None,
        time_convention: Optional[Union[str, TimeConvention]] = None,
        spot_forward: Optional[Union[str, SpotForward]] = None,
        forward_tenor: Optional[float] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: The client used to retrieve data.
            curves:
                Name of the curves that should be retrieved. Can be a single curve name
                as a string, a CurveName enum value, a list of curve names as strings,
                a list of CurveName enum values, or a list of strings or CurveName enum
                values mixed together.
            calc_date:
                The calculation date for the request.
            tenor_frequency:
                The frequency between tenors as a fraction of a year. Default is None.
            curve_type:
                The type of curve to be retrieved. Default is None.
            time_convention:
                The time convention used when the curve is constructed. Default is None.
            spot_forward:
                The type of curve to retrieve - spot, spot forward, or implied forward.
                Default is None.
            forward_tenor:
                The forward tenor for a forward curve or implied forward curve. Default is None.
        """
        ValueRetriever.__init__(self, client)
        self._client = client
        self.curves_original: List = curves if isinstance(curves, list) else [curves]
        self.curves = [
            (
                convert_to_variable_string(curve, CurveName)
                if isinstance(curve, CurveName)
                else curve
            )
            for curve in self.curves_original
        ]
        self.calc_date = calc_date
        self.tenor_frequency = (
            str(tenor_frequency) if tenor_frequency is not None else None
        )
        self.curve_type = (
            convert_to_variable_string(curve_type, CurveType)
            if curve_type is not None
            else None
        )
        self.time_convention = (
            convert_to_variable_string(time_convention, TimeConvention)
            if time_convention is not None
            else None
        )
        self.spot_forward = (
            convert_to_variable_string(spot_forward, SpotForward)
            if spot_forward is not None
            else None
        )
        self.forward_tenor = self.check_forward(forward_tenor)

        self._data = self.get_curve()

    def get_curve(self) -> List:
        """Retrieves response with curve.

        Returns:
            List of JSON response containing curve data.
        """
        json_response: List[Any] = []
        for request_dict in self.request:
            _json_response = self.get_response(request_dict)
            # To throw warning if curve in get_curve_time_series could not be retrieved
            CustomWarningCheck.curve_not_retrieved_warning(
                _json_response, request_dict["curve"]
            )

            json_map = _json_response[config["results"]["curve"]]
            json_response.append(json_map)

        return json_response

    def check_forward(self, forward_tenor: Union[float, None]) -> Union[str, None]:
        """Checks whether forward tenor is required and returns it as a string.

        Args:
            forward_tenor: Forward tenor value to check.

        Returns:
            A string representation of forward tenor or None if not required.

        Raises:
            AnalyticsInputError: If forward tenor is required but not provided.
        """
        if forward_tenor is None:
            if self.spot_forward in ["Forward", "ImpliedForward"]:
                raise AnalyticsInputError(
                    "Forward tenor has to be set for forward and implied forward curves"
                )
            else:
                return None
        else:
            return str(forward_tenor)

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method.

        Returns:
            Url suffix for the curve.
        """
        return config["url_suffix"]["curve"]

    @property
    def request(self) -> List[Dict]:
        """Request dictionary with curve.

        Returns:
            List of request dictionaries for each curve.
        """
        request_list = [
            {
                "date": self.calc_date.strftime("%Y-%m-%d"),
                "tenor-frequency": self.tenor_frequency,
                "curve": curve,
                "type": self.curve_type,
                "time-convention": self.time_convention,
                "spot-forward": self.spot_forward,
                "forward": self.forward_tenor,
            }
            for curve in self.curves
        ]

        return request_list

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary.

        Returns:
            Dictionary containing the reformatted curve data.
        """
        _dict = {}
        for curve in self._data:
            if len(curve["curve"]["curve_specification"]) > 0:
                _curve = {
                    "Type": curve["curve"]["curve_specification"]["type"],
                    "Time_convention": curve["curve"]["curve_specification"][
                        "time_convention"
                    ],
                    "Level": [
                        {
                            "Tenor": x["tenor"],
                            "Value": convert_to_float_if_float(x["value"]),
                        }
                        for x in curve["curve"]["values"]
                    ],
                }
                curve_name = convert_to_original_format(
                    curve["curve"]["curve_specification"]["name"], self.curves_original
                )
                _dict[curve_name] = _curve

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame.

        Returns:
            Pandas DataFrame containing the reformatted curve data.
        """
        return pd.DataFrame.from_dict(self.to_dict(), orient="index")
