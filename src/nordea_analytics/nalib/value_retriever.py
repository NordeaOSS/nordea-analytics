from abc import ABC, abstractmethod
import copy
from datetime import datetime
from functools import reduce
import json
import math
import operator
from typing import Any, Dict, List, Mapping, Optional, Union
import warnings

import numpy as np
import pandas as pd

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
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
    LiveDataRetrievalServiceClient,
)
from nordea_analytics.nalib.streaming_service import StreamListener
from nordea_analytics.nalib.util import (
    convert_to_float_if_float,
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.search_bond_names import (
    AmortisationType,
    AssetType,
    CapitalCentres,
    CapitalCentreTypes,
    Issuers,
)

config = get_config()


class ValueRetriever(ABC):
    """Base class for retrieving values from the DataRetrievalServiceClient."""

    def __init__(self, client: DataRetrievalServiceClient) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
        """
        self._client = client

    def get_response(self, request: Dict) -> Dict:
        """Calls the DataRetrievalServiceClient to get a response from the service.

        Args:
            request: request dictionary.

        Returns:
            Response from the service for a given method and request.
        """
        json_response = self._client.get_response(request, self.url_suffix)

        if "failed_queries" in json_response["data"].keys():
            if not json_response["data"]["failed_queries"] == []:
                warnings.warn(str(json_response["data"]["failed_queries"]))
        return json_response

    @property
    @abstractmethod
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        pass

    @property
    @abstractmethod
    def request(self) -> Union[Dict, List[Dict]]:
        """Creates a request dictionary for a given method."""
        pass

    @abstractmethod
    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        pass

    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        pass


class BondKeyFigures(ValueRetriever):
    """Retrieves and reformat given bond key figures for given ISINs and calc date."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        isins: Union[List[str], str],
        keyfigures: Union[
            str,
            BondKeyFigureName,
            List[str],
            List[BondKeyFigureName],
            List[Union[str, BondKeyFigureName]],
        ],
        calc_date: datetime,
    ) -> None:
        """Initialization of class.

        Args:
            client:  DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing
            isins: ISINs for requests.
            keyfigures: Bond key figure names for request.
            calc_date: calculation date for request.
        """
        super(BondKeyFigures, self).__init__(client)

        self.isins: List = [isins] if type(isins) != list else isins
        _keyfigures: List = keyfigures if type(keyfigures) == list else [keyfigures]
        self.keyfigures = [
            convert_to_variable_string(keyfigure, BondKeyFigureName)
            for keyfigure in _keyfigures
        ]
        self.calc_date = calc_date
        self._data = self.get_bond_key_figures()

    def get_bond_key_figures(self) -> List:
        """Calls the client and retrieves response with key figures from the service."""
        json_response: List[Any] = []
        for request_dict in self.request:
            _json_response = self.get_response(request_dict)
            json_map = reduce(
                operator.getitem, config["results"]["bond_key_figures"], _json_response
            )
            json_response = list(json_map) + json_response
        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["bond_key_figures"]

    @property
    def request(self) -> List[Dict]:
        """Request dictionary for a given set of ISINs, key figures and calc date."""
        if len(self.isins) > config["max_isins"]:
            split_isins = np.array_split(
                self.isins, math.ceil(len(self.isins) / config["max_isins"])
            )
            request_dict = [
                {
                    "symbols": list(isins),
                    "keyfigures": self.keyfigures,
                    "date": self.calc_date.strftime("%Y-%m-%d"),
                }
                for isins in split_isins
            ]
        else:
            request_dict = [
                {
                    "symbols": self.isins,
                    "keyFigures": self.keyfigures,
                    "date": self.calc_date.strftime("%Y-%m-%d"),
                }
            ]

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {}
        for isin_data in self._data:
            _isin_dict = {}
            for key_figure_data in isin_data["values"]:
                key_figure_name = BondKeyFigureName(key_figure_data["keyfigure"]).name
                _isin_dict[key_figure_name] = convert_to_float_if_float(
                    key_figure_data["value"]
                )

            _dict[isin_data["symbol"]] = _isin_dict

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        return pd.DataFrame.from_dict(self.to_dict(), orient="index")


class TimeSeries(ValueRetriever):
    """Retrieves and reformat time series."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        symbol: Union[List[str], str],
        keyfigures: Union[
            TimeSeriesKeyFigureName,
            str,
            List[str],
            List[TimeSeriesKeyFigureName],
            List[Union[str, TimeSeriesKeyFigureName]],
        ],
        from_date: datetime,
        to_date: datetime,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            symbol: Bonds ISINs, swaps, FX, FX swap point.
            keyfigures: Key figure names for request. If symbol is
                something else than a bond ISIN, quote should be chosen.
            from_date: From date for calc date interval.
            to_date: To date for calc date interval.
        """
        super(TimeSeries, self).__init__(client)
        self._client = client
        self.symbol: List = [symbol] if type(symbol) != list else symbol
        _keyfigures: List = keyfigures if type(keyfigures) == list else [keyfigures]
        self.keyfigures = [
            convert_to_variable_string(keyfigure, TimeSeriesKeyFigureName)
            for keyfigure in _keyfigures
        ]
        self.from_date = from_date
        self.to_date = to_date
        self._data = self.get_time_series()

    def get_time_series(self) -> List:
        """Retrieves response with key figures time series."""
        json_response: List[Any] = []
        for request_dict in self.request:
            _json_response = self.get_response(request_dict)
            json_map = reduce(
                operator.getitem, config["results"]["time_series"], _json_response
            )
            json_response = list(json_map) + json_response
        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["time_series"]

    @property
    def request(self) -> List[Dict]:
        """Request dictionary time series key figures."""
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

        split_symbol = np.array_split(
            self.symbol, math.ceil(len(self.symbol) / config["max_symbol_timeseries"])
        )

        request_dict = [
            {
                "symbols": list(symbol),
                "keyfigure": keyfigure,
                "from": dates["from"].strftime("%Y-%m-%d"),
                "to": dates["to"].strftime("%Y-%m-%d"),
            }
            for dates in date_interv
            for symbol in split_symbol
            for keyfigure in self.keyfigures
        ]

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict: Dict[Any, Any] = {}
        for isin_data in self._data:
            _timeseries_dict: Dict[Any, Any] = {}
            for timeseries in isin_data["timeseries"]:
                key_figure_name = BondKeyFigureName(timeseries["keyfigure"]).name
                _timeseries_dict[key_figure_name] = {}
                _timeseries_dict[key_figure_name]["Date"] = [
                    datetime.strptime(x["key"], "%Y-%m-%dT%H:%M:%S.0000000")
                    for x in timeseries["values"]
                ]
                _timeseries_dict[key_figure_name]["Value"] = [
                    convert_to_float_if_float(x["value"]) for x in timeseries["values"]
                ]

            if isin_data["symbol"] in _dict.keys():
                if key_figure_name in _dict[isin_data["symbol"]].keys():
                    if (
                        _dict[isin_data["symbol"]][key_figure_name]["Date"][-1]
                        > _timeseries_dict[key_figure_name]["Date"][0]
                    ):
                        _dict[isin_data["symbol"]][key_figure_name][
                            "Date"
                        ] += _timeseries_dict[key_figure_name]["Date"]
                        _dict[isin_data["symbol"]][key_figure_name][
                            "Value"
                        ] += _timeseries_dict[key_figure_name]["Value"]
                    else:
                        _dict[isin_data["symbol"]][key_figure_name]["Date"] = (
                            _timeseries_dict[key_figure_name]["Date"]
                            + _dict[isin_data["symbol"]][key_figure_name]["Date"]
                        )
                        _dict[isin_data["symbol"]][key_figure_name]["Value"] = (
                            _timeseries_dict[key_figure_name]["Value"]
                            + _dict[isin_data["symbol"]][key_figure_name]["Value"]
                        )
                else:
                    _dict[isin_data["symbol"]].update(_timeseries_dict)
            else:
                _dict[isin_data["symbol"]] = _timeseries_dict

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        df = pd.DataFrame.empty
        _dict = self.to_dict()
        for symbol in _dict:
            _df = pd.DataFrame.empty
            for keyfigure in _dict[symbol]:
                _df_keyfigure = pd.DataFrame.from_dict(_dict[symbol][keyfigure])
                _df_keyfigure = _df_keyfigure[["Date", "Value"]]
                _df_keyfigure.columns = ["Date", keyfigure]
                if _df is pd.DataFrame.empty:
                    _df = _df_keyfigure
                else:
                    _df = _df.merge(_df_keyfigure, on="Date", how="outer")
            _df = _df.sort_values(by="Date")
            _df.insert(0, "Symbol", [symbol] * len(_df))

            if df is pd.DataFrame.empty:
                df = _df
            else:
                df = df.append(_df)
        return df.reset_index(drop=True)


class IndexComposition(ValueRetriever):
    """Retrieves and reformat index composition for a given indices and calc date."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        indices: Union[List[str], str],
        calc_date: datetime,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            indices: Indices for request.
            calc_date: calculation date for request.
        """
        super(IndexComposition, self).__init__(client)
        self._client = client
        self.indices = indices
        self.calc_date = calc_date
        self._data = self.get_index_composition()

    def get_index_composition(self) -> Mapping:
        """Calls the client and retrieves response with index comp. from service."""
        json_response = self.get_response(self.request)
        return reduce(
            operator.getitem, config["results"]["index_composition"], json_response
        )

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["index_composition"]

    @property
    def request(self) -> Dict:
        """Request dictionary for a given set of indices and calc date."""
        return {
            "symbols": self.indices,
            "date": self.calc_date.strftime("%Y-%m-%d"),
        }

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {}
        for index_data in self._data:
            _isin_dict = {}
            _isin_dict["ISIN"] = [x["symbol"] for x in index_data["assets"]]
            _isin_dict["Name"] = [x["name"] for x in index_data["assets"]]
            _isin_dict["Nominal Amount"] = [
                convert_to_float_if_float(x["nominal"]) for x in index_data["assets"]
            ]
            sum_nominal = sum(_isin_dict["Nominal Amount"])
            _isin_dict["Nominal Weight"] = [
                x / sum_nominal for x in _isin_dict["Nominal Amount"]
            ]

            _isin_dict["Market Amount"] = [
                convert_to_float_if_float(x["market"]) for x in index_data["assets"]
            ]
            sum_market = sum(_isin_dict["Market Amount"])
            _isin_dict["Market Weight"] = [
                x / sum_market for x in _isin_dict["Market Amount"]
            ]
            _dict[index_data["index_name"]["name"]] = _isin_dict

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        df = pd.DataFrame.empty
        _dict = self.to_dict()
        for index in _dict:
            _df = pd.DataFrame.from_dict(_dict[index])
            _df.insert(0, "Index", [index] * len(_df))

            if df is pd.DataFrame.empty:
                df = _df
            else:
                df = df.append(_df)
        return df


class CurveTimeSeries(ValueRetriever):
    """Retrieves and reformat curve time series."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        curve: Union[str, CurveName],
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
            curve: Name of curve that should be retrieved.
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
        self.curve = curve
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
            json_map = reduce(
                operator.getitem, config["results"]["curve_time_series"], _json_response
            )
            json_response = list(json_map) + json_response
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

        request_dict = []
        for dates in date_interv:
            _initial_request_dict = {
                "from": dates["from"].strftime("%Y-%m-%d"),
                "to": dates["to"].strftime("%Y-%m-%d"),
                "curve": self.curve,
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

            request_dict.append(_request_dict)

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict: Dict[Any, Any] = {}
        curve_name = (
            self.curve if type(self.curve) == str else self.curve.name  # type:ignore
        )

        for timeseries in self._data:
            for tenor in timeseries["values"]:
                if self.forward_tenor is None:
                    curve_and_tenor = curve_name + "(" + str(tenor["tenor"]) + "Y)"
                else:
                    curve_and_tenor = (
                        curve_name
                        + "("
                        + self.forward_tenor
                        + "Y)"
                        + "("
                        + str(tenor["tenor"])
                        + "Y)"
                    )

                if curve_and_tenor not in _dict.keys():
                    _dict[curve_and_tenor] = {}
                    _dict[curve_and_tenor]["Value"] = [
                        convert_to_float_if_float(tenor["value"])
                    ]
                    _dict[curve_and_tenor]["Date"] = [
                        datetime.strptime(timeseries["date"].split("T")[0], "%Y-%m-%d")
                    ]
                else:
                    _dict[curve_and_tenor]["Value"].append(
                        convert_to_float_if_float(tenor["value"])
                    )
                    _dict[curve_and_tenor]["Date"].append(
                        datetime.strptime(timeseries["date"].split("T")[0], "%Y-%m-%d")
                    )

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        df = pd.DataFrame.empty
        _dict = self.to_dict()
        for curve_and_tenor in _dict:
            _df = pd.DataFrame.empty
            _df = pd.DataFrame.from_dict(_dict[curve_and_tenor])
            _df = _df[["Date", "Value"]]
            _df.columns = ["Date", curve_and_tenor]
            if df is pd.DataFrame.empty:
                df = _df
            else:
                df = df.merge(_df, on="Date", how="outer")
            df = df.sort_values(by="Date")

        return df


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
        self.curve = curve
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
        return reduce(operator.getitem, config["results"]["curve"], json_response)

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
                {"tenor": x["tenor"], "value": convert_to_float_if_float(x["value"])}
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


class CurveDefinition(ValueRetriever):
    """Retrieves and reformat curve definition."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        curve: Union[str, CurveName],
        calc_date: datetime,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            curve: Name of curve that should be retrieved.
            calc_date: calculation date for request.
        """
        super(CurveDefinition, self).__init__(client)
        self._client = client
        self.curve = curve
        self.calc_date = calc_date

        self._data = self.get_curve_definition()

    def get_curve_definition(self) -> Mapping:
        """Retrieves response with curve definition."""
        json_response = self.get_response(self.request)
        return reduce(
            operator.getitem, config["results"]["curve_definition"], json_response
        )

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["curve_definition"]

    @property
    def request(self) -> Dict:
        """Request dictionary curve time definition."""
        request = {"date": self.calc_date.strftime("%Y-%m-%d"), "curve": self.curve}

        return request

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {}
        _curve_def_dict: Dict[Any, Any] = {}
        for curve_def in self._data["values"]:
            _curve_def_dict = {}
            _curve_def_dict["Quote"] = convert_to_float_if_float(
                curve_def["asset"]["quote"]
            )
            _curve_def_dict["Weight"] = curve_def["asset"]["weight"]
            _curve_def_dict["Maturity"] = datetime.strptime(
                curve_def["asset"]["maturity"], "%Y-%m-%dT%H:%M:%S.0000000"
            )
            _dict[curve_def["name"]] = _curve_def_dict
        return {self.curve: _dict}

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()
        df = pd.DataFrame.from_dict(_dict[self.curve]).transpose()
        df = df.reset_index().rename(columns={"index": "Name"})
        df.index = [self.curve] * len(df)
        return df


class BondFinder(ValueRetriever):
    """Retrieves and reformat bonds given a search criteria."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        dmb: bool = False,
        country: Optional[str] = None,
        currency: Optional[str] = None,
        issuers: Optional[Union[List[Issuers], List[str], Issuers, str]] = None,
        asset_types: Optional[Union[List[AssetType], List[str], AssetType, str]] = None,
        lower_maturity: Optional[datetime] = None,
        upper_maturity: Optional[datetime] = None,
        lower_coupon: Optional[float] = None,
        upper_coupon: Optional[float] = None,
        amortisation_type: Optional[Union[AmortisationType, str]] = None,
        is_io: Optional[bool] = None,
        capital_centres: Optional[
            Union[List[str], str, CapitalCentres, List[CapitalCentres]]
        ] = None,
        capital_centre_types: Optional[
            Union[List[str], str, CapitalCentreTypes, List[CapitalCentreTypes]]
        ] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            dmb: Default to False. True if only Danish Mortgage
                Bonds should be found.
            country: country of issue.
            currency: issue currency.
            issuers: name of issuers.
            asset_types: Type of asset.
            lower_maturity: minimum(from) maturity.
            upper_maturity: maximum(to) maturity.
            lower_coupon: minimum coupon.
            upper_coupon: maximum coupon.
            amortisation_type: amortisation type of bond.
            is_io: Is Interest Only - only relevant for DMB.
            capital_centres: capital centres names - only relevant for DMB.
            capital_centre_types: capital centres types - only relevant for DMB.

        """
        super(BondFinder, self).__init__(client)
        self._client = client
        self.dmb = dmb
        self.country = country
        self.currency = currency
        self.issuers = issuers
        _asset_types: List = asset_types if type(asset_types) == list else [asset_types]
        self.asset_types = (
            [
                convert_to_variable_string(asset_type, AssetType)
                for asset_type in _asset_types
            ]
            if asset_types is not None
            else None
        )

        self.lower_maturity = (
            lower_maturity.strftime("%Y-%m-%d") if lower_maturity is not None else None
        )
        self.upper_maturity = (
            upper_maturity.strftime("%Y-%m-%d") if upper_maturity is not None else None
        )
        self.lower_coupon = str(lower_coupon) if lower_coupon is not None else None
        self.upper_coupon = str(upper_coupon) if upper_coupon is not None else None
        self.amortisation_type = (
            convert_to_variable_string(amortisation_type, AmortisationType)
            if amortisation_type is not None
            else None
        )
        self.is_io = str(is_io) if is_io is not None else None
        _capital_centres: List = (
            capital_centres if type(capital_centres) == list else [capital_centres]
        )
        self.capital_centres = (
            [
                convert_to_variable_string(capital_centre, CapitalCentres)
                for capital_centre in _capital_centres
            ]
            if capital_centres is not None
            else None
        )
        _capital_centre_types: List = (
            capital_centre_types
            if type(capital_centre_types) == list
            else [capital_centre_types]
        )
        self.capital_centre_types = (
            [
                convert_to_variable_string(capital_centre_type, CapitalCentreTypes)
                for capital_centre_type in _capital_centre_types
            ]
            if capital_centre_types is not None
            else None
        )
        self.check_inputs()

        self._data = self.get_search_bonds()

    def get_search_bonds(self) -> Mapping:
        """Retrieves response given the search criteria."""
        json_response = self.get_response(self.request)
        return reduce(operator.getitem, config["results"]["search"], json_response)

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        if self.dmb:
            return config["url_suffix"]["search_dmb_bonds"]
        else:
            return config["url_suffix"]["search_bonds"]

    @property
    def request(self) -> Dict:
        """Request dictionary for searched bonds."""
        initial_request = {
            "country": self.country,
            "currency": self.currency,
            "issuers": self.issuers,
            "asset-types": self.asset_types,
            "lower-maturity": self.lower_maturity,
            "upper-maturity": self.upper_maturity,
            "lower-coupon": self.lower_coupon,
            "upper-coupon": self.upper_coupon,
            "amortisation-type": self.amortisation_type,
            "is-io": self.is_io,
            "capital-centres": self.capital_centres,
            "capital-centretypes": self.capital_centre_types,
        }

        request = {
            key: initial_request[key]
            for key in initial_request.keys()
            if initial_request[key] is not None
        }
        if request == {}:
            raise ValueError("You need to input some search criteria")
        return request

    def check_inputs(self) -> None:
        """Check if inputs are given that only apply to dmb."""
        if self.is_io is not None:
            if not self.dmb:
                warnings.warn(
                    "is_io is only relevant for DMB. This variable will be ignored."
                )
        if self.capital_centres is not None:
            if not self.dmb:
                warnings.warn(
                    "capital_centres is only relevant for DMB. "
                    "This variable will be ignored."
                )
        if self.capital_centre_types is not None:
            if not self.dmb:
                warnings.warn(
                    "capital_centre_types is only relevant for DMB."
                    " This variable will be ignored."
                )

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict: Dict[Any, Any] = {}
        for i, search_data in enumerate(self._data):
            _dict[i] = {}
            _dict[i]["ISIN"] = search_data["isin"]
            _dict[i]["Name"] = search_data["name"]
        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()
        df = pd.DataFrame.from_dict(_dict).transpose()
        return df


class BondKeyFigureCalculator(ValueRetriever):
    """Retrieves and reformat calculated bond key figure."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        isin: str,
        keyfigures: Union[
            str,
            CalculatedBondKeyFigureName,
            List[str],
            List[CalculatedBondKeyFigureName],
            List[Union[str, CalculatedBondKeyFigureName]],
        ],
        calc_date: datetime,
        curves: Optional[Union[List[str], str, CurveName, List[CurveName]]] = None,
        rates_shifts: Optional[Union[List[str], str]] = None,
        pp_speed: Optional[float] = None,
        price: Optional[float] = None,
        spread: Optional[float] = None,
        spread_curve: Optional[Union[str, CurveName]] = None,
        asw_fix_frequency: Optional[str] = None,
        ladder_definition: Optional[List[str]] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            isin: ISIN of bond that should be valued.
            keyfigures: Bond key figure that should be valued.
            calc_date: date of calculation
            curves: discount curves for calculation
            rates_shifts: shifts in curves("tenor shift in bbp"
                like "0Y 5" or "30Y -5").
            pp_speed: Prepayment speed. Default = 1.
            price: fixed price for ISIN
            spread: fixed spread for ISIN. Mandatory to give
                spread_curve also as an input.
            spread_curve: spread curve to calculate the
                key figures when a fixed spread is given.
            asw_fix_frequency: Fixing frequency of swap in ASW calculation.
                Mandatory input in all ASW calculations.
            ladder_definition: What tenors should be included in BPV ladder calculation.
        """
        super(BondKeyFigureCalculator, self).__init__(client)
        self._client = client
        _keyfigures: List = keyfigures if type(keyfigures) == list else [keyfigures]
        self.keyfigures = [
            convert_to_variable_string(keyfigure, CalculatedBondKeyFigureName)
            for keyfigure in _keyfigures
        ]
        self.isin = isin
        self.calc_date = calc_date
        self.curves = curves
        self.rates_shifts = rates_shifts
        self.pp_speed = pp_speed
        self.price = price
        self.spread = spread
        self.spread_curve = spread_curve
        self.asw_fix_frequency = asw_fix_frequency
        self.ladder_definition = ladder_definition

        self._data = self.calculate_bond_key_figure()

    def calculate_bond_key_figure(self) -> Mapping:
        """Retrieves response with calculated key figures."""
        json_response = self.get_post_get_response()
        return json_response

    def get_post_get_response(self) -> Dict:
        """Retrieves response after posting the request."""
        return self._client.get_post_get_response(self.request, self.url_suffix)

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["calculate"]

    @property
    def request(self) -> Dict:
        """Post request dictionary calculate bond key figure."""
        keyfigures = copy.deepcopy(self.keyfigures)
        keyfigures.remove("price") if "price" in self.keyfigures else keyfigures
        if keyfigures == []:  # There has to be some key figure in request,
            # but it will not be returned in final results
            keyfigures = "bpv"  # type:ignore
        initial_request = {
            "symbol": self.isin,
            "date": self.calc_date.strftime("%Y-%m-%d"),
            "keyfigures": keyfigures,
            "curves": self.curves,
            "rates_shift": self.rates_shifts,
            "pp_speed": self.pp_speed,
            "price": self.price,
            "spread": self.spread,
            "spread_curve": self.spread_curve,
            "asw_fix_frequency": self.asw_fix_frequency,
            "ladder_definition": self.ladder_definition,
        }
        request = {
            key: initial_request[key]
            for key in initial_request.keys()
            if initial_request[key] is not None
        }
        return request

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict: Dict[Any, Any] = {}
        for key_figure in self._data:
            if key_figure != "price" and key_figure in self.keyfigures:
                for curve_data in self._data[key_figure]["values"]:
                    _data_dict = {}
                    _data_dict[key_figure.capitalize()] = convert_to_float_if_float(
                        curve_data["value"]
                    )
                    if curve_data["key"] in _dict.keys():
                        _dict[curve_data["key"]].update(_data_dict)
                    else:
                        _dict[curve_data["key"]] = _data_dict
        if (
            _dict == {}
        ):  # This would be the case if only Price would be selected as key figure
            for curve_data in self._data["bpv"]["values"]:
                _dict[curve_data["key"]] = {}

        if "price" in self._data and "price" in self.keyfigures:
            for curve in _dict:
                _dict[curve]["Price"] = self._data["price"]
        return {self.isin: _dict}

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()
        df = pd.DataFrame.from_dict(_dict[self.isin]).transpose()
        df = df.reset_index().rename(columns={"index": "Curve"})
        df.index = [self.isin] * len(df)
        return df


class BondKeyFigureHorizonCalculator(ValueRetriever):
    """Retrieves and reformat calculated future bond key figure."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        isin: str,
        keyfigures: Union[
            str,
            HorizonCalculatedBondKeyFigureName,
            List[str],
            List[HorizonCalculatedBondKeyFigureName],
            List[Union[str, HorizonCalculatedBondKeyFigureName]],
        ],
        calc_date: datetime,
        horizon_date: datetime,
        curves: Optional[Union[List[str], str, CurveName, List[CurveName]]] = None,
        rates_shifts: Optional[Union[List[str], str]] = None,
        pp_speed: Optional[float] = None,
        price: Optional[float] = None,
        fixed_prepayments: Optional[float] = None,
        reinvest_in_series: Optional[bool] = None,
        reinvestment_rate: Optional[float] = None,
        spread_change_horizon: Optional[float] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            isin: ISIN of bond that should be valued.
            keyfigures: Bond key figure that should be valued.
            calc_date: date of calculation
            horizon_date: future date of calculation
            curves: discount curves for calculation
            rates_shifts: shifts in curves("tenor shift in bbp"
                like "0Y 5" or "30Y -5").
            pp_speed: Prepayment speed. Default = 1.
            price: fixed price for ISIN.
            fixed_prepayments: repayments between calc_cate and horizon date.
                Value of 0.01 would mean that prepayments are set to 1%,
                but model prepayments are still used after horizon date.
                If noting entered, then model prepayments used.
            reinvest_in_series:  True if you want to reinvest in the series.
                Default value is True
            reinvestment_rate: Rate you want to reinvest if you don't
                want to reinvest in series. Only relevant if
                    reinvest_in_series is False, or horizon date is
                    further out than maturity of the bond.
            spread_change_horizon:bump the spread between calc date
                and horizon date. Values should be in bps.
        """
        super(BondKeyFigureHorizonCalculator, self).__init__(client)
        self._client = client
        _keyfigures: List = keyfigures if type(keyfigures) == list else [keyfigures]
        self.keyfigures = [
            convert_to_variable_string(keyfigure, HorizonCalculatedBondKeyFigureName)
            for keyfigure in _keyfigures
        ]
        self.isin = isin
        self.calc_date = calc_date
        self.horizon_date = horizon_date
        self.curves = curves
        self.rates_shifts = rates_shifts
        self.pp_speed = pp_speed
        self.price = price
        self.fixed_prepayments = fixed_prepayments
        self.reinvest_in_series = reinvest_in_series
        self.reinvestment_rate = reinvestment_rate
        self.spread_change_horizon = spread_change_horizon

        self._data = self.calculate_bond_key_figure()

    def calculate_bond_key_figure(self) -> Mapping:
        """Retrieves response with calculated key figures."""
        json_response = self.get_post_get_response()
        return json_response

    def get_post_get_response(self) -> Dict:
        """Retrieves response after posting the request."""
        return self._client.get_post_get_response(self.request, self.url_suffix)

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["calculate_horizon"]

    @property
    def request(self) -> Dict:
        """Post request dictionary calculate bond key figure."""
        keyfigures = copy.deepcopy(self.keyfigures)
        keyfigures.remove("price") if "price" in self.keyfigures else keyfigures
        if keyfigures == []:  # There has to be some key figure in request,
            # but it will not be returned in final results
            keyfigures = "bpv"  # type:ignore
        initial_request = {
            "symbol": self.isin,
            "date": self.calc_date.strftime("%Y-%m-%d"),
            "horizon_date": self.horizon_date.strftime("%Y-%m-%d"),
            "keyfigures": keyfigures,
            "curves": self.curves,
            "rates_shift": self.rates_shifts,
            "pp_speed": self.pp_speed,
            "price": self.price,
            "fixed_prepayments": self.fixed_prepayments,
            "reinvest_in_series": self.reinvest_in_series,
            "reinvestment_rate": self.reinvestment_rate,
            "spread_change_horizon": self.spread_change_horizon,
        }
        request = {
            key: initial_request[key]
            for key in initial_request.keys()
            if initial_request[key] is not None
        }
        return request

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict: Dict[Any, Any] = {}
        for key_figure in self._data:
            if (
                "price" not in key_figure
                and key_figure != "error"
                and key_figure in self.keyfigures
            ):

                for curve_data in self._data[key_figure]["values"]:
                    _data_dict = {}
                    _data_dict[key_figure.capitalize()] = convert_to_float_if_float(
                        curve_data["value"]
                    )
                    if curve_data["key"] in _dict.keys():
                        _dict[curve_data["key"]].update(_data_dict)
                    else:
                        _dict[curve_data["key"]] = _data_dict

        if (
            _dict == {}
        ):  # This would be the case if only Price would be selected as key figure
            for curve_data in self._data["bpv"]["values"]:
                _dict[curve_data["key"]] = {}

        if "price" in self._data and "price" in self.keyfigures:
            for curve in _dict:
                _dict[curve]["Price"] = self._data["price"]

        return {self.isin: _dict}

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()
        df = pd.DataFrame.from_dict(_dict[self.isin]).transpose()
        df = df.reset_index().rename(columns={"index": "Curve"})
        df.index = [self.isin] * len(df)
        return df


class LiveBondKeyFigures(ValueRetriever):
    """Retrieves and reformats calculated live bond key figure."""

    def __init__(
        self,
        client: LiveDataRetrievalServiceClient,
        isins: Union[str, List[str]],
        keyfigures: Union[
            List[LiveBondKeyFigureName], List[str], LiveBondKeyFigureName, str
        ],
        _dict: Dict,
        as_df: bool,
    ) -> None:
        """Initialization of class.

        Args:
            client: LiveDataRetrivalServiceClient
            isins: ISINs of bond that should be retrieved live
            keyfigures: List of bond key figures which should be streamed.
                Can be a list of LiveBondKeyFigureNames or string.
            _dict: Dictionary with live values.
            as_df: if True, the results are represented
                as pd.DataFrame, else as dictionary
        """
        super(LiveBondKeyFigures, self).__init__(client)
        self.isins = [isins] if type(isins) == str else isins
        _keyfigures: List = keyfigures if type(keyfigures) == list else [keyfigures]
        self.keyfigures = [
            convert_to_variable_string(keyfigure, LiveBondKeyFigureName)
            for keyfigure in _keyfigures
        ]
        self.as_df = as_df
        self._data: Dict[Any, Any] = {}
        self.dict = _dict

    def get_live_streamer(self) -> StreamListener:
        """Returns the stream listener which controls the live stream."""
        return self._client.get_live_streamer(
            self.request, self.url_suffix, self.update_live_streaming
        )

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["live_bond_key_figures"]

    @property
    def request(self) -> Dict:
        """Request dictionary for a given set of ISINs, key figures and calc date."""
        request_dict = {i: x for i, x in enumerate(self.isins)}
        return request_dict

    def update_live_streaming(self, results: Union[Dict, str]) -> Any:
        """Streamed data is transformed into a presentable format."""
        if type(results) == str:
            self._data = json.loads(results)
            self.to_dict()
        elif type(results) == dict:
            self._data = results
            self.to_dict_external()

        if self.as_df:
            return self.to_df()
        else:
            return self.dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {}

        for key_figure_data in self._data["values"]:
            key_figure_name = key_figure_data["keyfigure"].lower()
            if key_figure_name in self.keyfigures:
                _dict[key_figure_name.capitalize()] = convert_to_float_if_float(
                    key_figure_data["value"]
                )
                _dict["timestamp"] = str(
                    datetime.fromtimestamp(key_figure_data["timestamp"])
                )

        self.dict[self._data["isin"]] = _dict
        return self.dict

    def to_dict_external(self) -> Dict:
        """Reformat the json response to a dictionary for external stream."""
        for isin_data in self._data["data"]["keyfigure_values"]:
            _dict = {}
            for key_figure_data in isin_data["values"]:
                key_figure_name = key_figure_data["keyfigure"].lower()
                if key_figure_name in self.keyfigures:
                    _dict[key_figure_name.capitalize()] = convert_to_float_if_float(
                        key_figure_data["value"]
                    )
                    _dict["timestamp"] = str(
                        datetime.fromtimestamp(key_figure_data["updated_at"])
                    )

            self.dict[isin_data["isin"]] = _dict
        return self.dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _df = pd.DataFrame.from_dict(self.dict, orient="index")
        col = _df.pop("timestamp")
        _df.insert(len(_df.columns), col.name, col)
        _df.index.name = "ISIN"
        return _df.reset_index()


class FXForecast(ValueRetriever):
    """Retrieves and reformat FX forecast."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        currency_pair: str,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            currency_pair: Currencycross for which to retrieve forecasts.
        """
        super(FXForecast, self).__init__(client)
        self._client = client
        self.currency_pair = currency_pair
        self._data = self.get_fx_forecast()

    def get_fx_forecast(self) -> Mapping:
        """Retrieves response with FX forecast."""
        json_response = self.get_response(self.request)
        return reduce(operator.getitem, config["results"]["fx_forecast"], json_response)

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method."""
        return config["url_suffix"]["fx_forecast"]

    @property
    def request(self) -> Dict:
        """Request dictionary FX forecast."""
        currency_pair = self.currency_pair

        request_dict = {
            "currency-pair": currency_pair,
        }

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict: Dict[Any, Any] = {}

        forecast_data = {}
        for data in self._data["forecast"]:
            values = {}
            values["updated_at"] = datetime.strptime(
                self._data["updated_at"].split("T")[0], "%Y-%m-%d"
            )
            values["horizon_date"] = datetime.strptime(
                data["horizon_date"].split("T")[0], "%Y-%m-%d"
            )
            values["forecast"] = data["value"]
            forecast_data[data["tenor"]] = values

        _dict[self._data["symbol"]] = forecast_data

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()

        df = pd.DataFrame.from_dict(
            {(i, j): _dict[i][j] for i in _dict.keys() for j in _dict[i].keys()},
            orient="index",
        )
        df = df.reset_index().rename(
            columns={"level_0": "currency_pair", "level_1": "tenor"}
        )

        return df
