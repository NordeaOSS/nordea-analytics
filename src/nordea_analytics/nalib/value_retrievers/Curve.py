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
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class Curve(ValueRetriever):
    """Retrieves and reformat curves."""

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
        curve_type: Union[str, CurveType] = None,
        tenor_frequency: float = None,
        time_convention: Union[str, TimeConvention] = None,
        spot_forward: Optional[Union[str, SpotForward]] = None,
        forward_tenor: Optional[float] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            curves: Name of curves that should be retrieved.
            calc_date: calculation date for request.
            tenor_frequency: frequency between tenors as a fraction of a year.
            curve_type: What type of curve is retrieved.
            time_convention: Time convention used when curve is constructed.
            spot_forward: Should the curve be spot, spot forward
                 or implied forward.
            forward_tenor: Forward tenor for forward curve or implied forward curve.
        """
        super(Curve, self).__init__(client)
        self._client = client
        _curves: List = curves if type(curves) == list else [curves]
        self.curves = [
            convert_to_variable_string(curve, CurveName)
            if type(curve) == CurveName
            else curve
            for curve in _curves
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

        result = self.get_curve()

        self._data = self.format_curve_names(result, _curves)

    def get_curve(self) -> List:
        """Retrieves response with curve."""
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

    def format_curve_names(
        self,
        data: List,
        curves: Union[List[str], List[CurveName], List[Union[str, CurveName]]],
    ) -> List:
        """Formats curve names to be identical to curves input."""
        curve_dict = {}
        for curve_name in curves:
            curve_name_string: Union[str, ValueError]
            if type(curve_name) == CurveName:
                curve_name_string = convert_to_variable_string(
                    curve_name, CurveName
                ).upper()
            elif type(curve_name) == str:
                curve_name_string = curve_name.upper()
            curve_dict[curve_name_string] = (
                curve_name.name if type(curve_name) == CurveName else curve_name
            )

        for curve_result in data:
            if "name" in curve_result["curve"]["curve_specification"]:
                curve_result["curve"]["curve_specification"]["name"] = curve_dict[
                    curve_result["curve"]["curve_specification"]["name"].upper()
                ]

        return data

    def check_forward(self, forward_tenor: Union[float, None]) -> Union[str, None]:
        """Check if forward tenor should be given as an argument.

        Args:
            forward_tenor: Given forward tenor to service.

        Returns:
            Forward tenor as a string or None.

        Raises:
            AnalyticsInputError: If forward tenor should have a value.
        """
        if forward_tenor is None:
            if self.spot_forward == "Forward" or self.spot_forward == "ImpliedForward":
                raise AnalyticsInputError(
                    "Forward tenor has to be set for forward and"
                    " implied forward curves"
                )
            else:
                return None
        else:
            return str(forward_tenor)

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["curve"]

    @property
    def request(self) -> List[Dict]:
        """Request dictionary with curve."""
        request_list = [
            {
                "date": self.calc_date.strftime("%Y-%m-%d"),
                "tenor-frequency": self.tenor_frequency,
                "curve": curve,
                "type": self.curve_type,
                "time-convention": self.time_convention,
                "spot-forward": self.spot_forward,
                "Forward": self.forward_tenor,
            }
            for curve in self.curves
        ]

        return request_list

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
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

                _dict[curve["curve"]["curve_specification"]["name"]] = _curve

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        return pd.DataFrame.from_dict(self.to_dict(), orient="index")
