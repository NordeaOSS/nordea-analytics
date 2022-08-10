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
from nordea_analytics.nalib.exceptions import AnalyticsInputError
from nordea_analytics.nalib.util import (
    check_json_response,
    check_json_response_error,
    convert_to_float_if_float,
    convert_to_variable_string,
    float_to_tenor_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class CurveTimeSeries(ValueRetriever):
    """Retrieves and reformat curve time series."""

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
        from_date: datetime,
        to_date: datetime,
        tenors: Union[float, List[float]],
        curve_type: Union[str, CurveType] = None,
        time_convention: Union[str, TimeConvention] = None,
        spot_forward: Optional[Union[str, SpotForward]] = None,
        forward_tenor: Optional[float] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            curves: Name of curves that should be retrieved.
            from_date: From date for calc date interval.
            to_date: To date for calc date interval.
            tenors: For what tenors should be curve be constructed.
            curve_type: What type of curve is retrieved.
            time_convention: Time convention used when curve is constructed.
            spot_forward: Should the curve be spot, spot forward
                 or implied forward.
            forward_tenor: Forward tenor for forward curve or implied forward curve.
        """
        super(CurveTimeSeries, self).__init__(client)
        self._client = client
        _curves: List = curves if type(curves) == list else [curves]
        self.curves = [
            convert_to_variable_string(curve, CurveName) for curve in _curves
        ]
        self.from_date = from_date
        self.to_date = to_date
        _tenors: List = tenors if type(tenors) == list else [tenors]  # type:ignore
        self.tenors = [str(t) for t in _tenors]
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
        self._data = self.get_curve_time_series()

    def get_curve_time_series(self) -> List:
        """Retrieves response with curve time series."""
        json_response: List[Any] = []
        for request_dict in self.request:
            _json_response = self.get_response(request_dict)
            json_map = _json_response[config["results"]["curve_time_series"]]
            json_response.append({"curve": request_dict["curve"], "values": json_map})

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
            AnalyticsInputError: If forward tenor should have a value.
        """
        if forward_tenor is None:
            if self.spot_forward == "Forward" or self.spot_forward == "ImpliedForward":
                raise AnalyticsInputError(
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
        return config["url_suffix"]["curve_time_series"]

    @property
    def request(self) -> List[Dict]:
        """Request dictionary curve time series."""
        intv = config["max_years_timeseries"] * 365
        date_interv = []
        new_from_date = self.from_date
        while (self.to_date - new_from_date).days > intv:
            new_to_date = new_from_date.replace(
                year=new_from_date.year + config["max_years_timeseries"]
            )
            date_interv.append({"from": new_from_date, "to": new_to_date})
            new_from_date = new_to_date.replace(day=new_to_date.day + 1)
        date_interv.append({"from": new_from_date, "to": self.to_date})

        request_list = []

        for curve in self.curves:
            for dates in date_interv:
                _initial_request_dict = {
                    "from": dates["from"].strftime("%Y-%m-%d"),
                    "to": dates["to"].strftime("%Y-%m-%d"),
                    "curve": curve,
                    "tenors": self.tenors,
                    "type": self.curve_type,
                    "time-convention": self.time_convention,
                    "spot-forward": self.spot_forward,
                    "forward": self.forward_tenor,
                }

                _request_dict = {
                    key: _initial_request_dict[key]
                    for key in _initial_request_dict.keys()
                    if _initial_request_dict[key] is not None
                }

                request_list.append(_request_dict)

        return request_list

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _curves_dict: Dict[Any, Any] = {}
        for curve_series in self._data:
            _tenor_dict: Dict[Any, Any] = {}
            for timeseries in curve_series["values"]:
                for tenor in timeseries["values"]:
                    if self.forward_tenor is None:
                        curve_and_tenor = (
                            curve_series["curve"]
                            + "("
                            + float_to_tenor_string(tenor["tenor"])
                            + ")"
                        )
                    else:
                        curve_and_tenor = (
                            curve_series["curve"]
                            + "("
                            + float_to_tenor_string(self.forward_tenor)
                            + ")"
                            + "("
                            + float_to_tenor_string(tenor["tenor"])
                            + ")"
                        )

                    if curve_and_tenor not in _tenor_dict.keys():
                        _tenor_dict[curve_and_tenor] = {}
                        _tenor_dict[curve_and_tenor]["Value"] = [
                            convert_to_float_if_float(tenor["value"])
                        ]
                        _tenor_dict[curve_and_tenor]["Date"] = [
                            datetime.strptime(
                                timeseries["date"].split("T")[0], "%Y-%m-%d"
                            )
                        ]
                    else:
                        _tenor_dict[curve_and_tenor]["Value"].append(
                            convert_to_float_if_float(tenor["value"])
                        )
                        _tenor_dict[curve_and_tenor]["Date"].append(
                            datetime.strptime(
                                timeseries["date"].split("T")[0], "%Y-%m-%d"
                            )
                        )
            _curves_dict[curve_series["curve"]] = _tenor_dict

        return _curves_dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        df = pd.DataFrame.empty
        _dict = self.to_dict()

        _df = pd.DataFrame.from_dict(_dict, orient="index")
        for curve_series in _dict:
            for tenor_series in _dict[curve_series]:
                _df = pd.DataFrame.empty
                _df = pd.DataFrame.from_dict(_dict[curve_series][tenor_series])
                _df = _df[["Date", "Value"]]
                _df.columns = ["Date", tenor_series]
                if df is pd.DataFrame.empty:
                    df = _df
                else:
                    df = df.merge(_df, on="Date", how="outer")
                df = df.sort_values(by="Date")

        return df
