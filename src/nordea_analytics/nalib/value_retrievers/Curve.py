from datetime import datetime
from typing import Dict, Mapping, Optional, Union

import pandas as pd

from nordea_analytics.curve_variable_names import (
    CurveName,
    CurveType,
    SpotForward,
    TimeConvention,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    check_json_response,
    check_json_response_error,
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
        curve: Union[str, CurveName],
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
            curve: Name of curve that should be retrieved.
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
        self.curve = convert_to_variable_string(curve, CurveName)
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

    def get_curve(self) -> Mapping:
        """Retrieves response with curve."""
        json_response = self.get_response(self.request)
        json_response = json_response[config["results"]["curve"]]

        output_found = check_json_response(json_response)
        check_json_response_error(output_found)

        return json_response

    def check_forward(self, forward_tenor: Union[float, None]) -> Union[str, None]:
        """Check if forward tenor should be given as an argument.

        Args:
            forward_tenor: Given forward tenor to service.

        Returns:
            Forward tenor as a string or None.

        Raises:
            ValueError: If forward tenor should have a value.
        """
        if forward_tenor is None:
            if self.spot_forward == "Forward" or self.spot_forward == "ImpliedForward":
                raise ValueError(
                    "Forward tenor has to be chosen for forward and"
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
    def request(self) -> Dict:
        """Request dictionary with curve."""
        initial_request = {
            "date": self.calc_date.strftime("%Y-%m-%d"),
            "tenor-frequency": self.tenor_frequency,
            "curve": self.curve,
            "type": self.curve_type,
            "time-convention": self.time_convention,
            "spot-forward": self.spot_forward,
            "Forward": self.forward_tenor,
        }

        request = {
            key: initial_request[key]
            for key in initial_request.keys()
            if initial_request[key] is not None
        }

        return request

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {
            self.curve: [
                {"Tenor": x["tenor"], "Value": convert_to_float_if_float(x["value"])}
                for x in self._data["curve"]["values"]
            ]
        }
        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()
        df = pd.DataFrame.from_dict(_dict[self.curve])
        df.index = [self.curve] * len(df)
        return df
