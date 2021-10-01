from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from nordea_analytics.bond_key_figure_name import BondKeyFigureName
from nordea_analytics.curve_variable_names import (
    CurveType,
    SpotForward,
    TimeConvention,
)
from nordea_analytics.nalib.data_retrieval_client import DataRetrievalServiceClient
from nordea_analytics.nalib.util import (
    convert_to_float_if_float,
    convert_to_variable_string,
)


class ValueRetriever(ABC):
    """Base class for retrieving values from the DataRetrievalServiceClient."""

    def __init__(self, client: DataRetrievalServiceClient) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientFile for testing.
        """
        self._client = client

    def get_response(self) -> Dict:
        """Calls the DataRetrievalServiceClient to get a response from the serivce.

        Returns:
            Response from the service for a given method and request.
        """
        json_response = self._client.get_response(self.request, self.url_suffix)
        return json_response

    @property
    @abstractmethod
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        pass

    @property
    @abstractmethod
    def request(self) -> Dict:
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
        isins: List[str],
        keyfigures: Union[
            List[str], List[BondKeyFigureName], List[Union[str, BondKeyFigureName]]
        ],
        calc_date: datetime,
    ) -> None:
        """Initialization of class.

        Args:
            client:  DataRetrievalServiceClient
                or DataRetrievalServiceClientFile for testing
            isins: ISINs for requests.
            keyfigures: Bond key figure names for request.
            calc_date: calculation date for request.
        """
        super(BondKeyFigures, self).__init__(client)

        self.isins = ",".join(isins) if len(isins) > 1 else isins
        self.keyfigures = [
            convert_to_variable_string(keyfigure, BondKeyFigureName)
            for keyfigure in keyfigures
        ]
        self.calc_date = calc_date
        self._data = self.get_bond_key_figures()

    def get_bond_key_figures(self) -> Dict:
        """Calls the client and retrieves response with key figures from the service."""
        json_response = self.get_response()
        return json_response["response"]

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return "instruments/keyfigure/"

    @property
    def request(self) -> Dict:
        """Request dictionary for a given set of ISINs, key figures and calc date."""
        return {
            "symbols": self.isins,
            "keyFigures": self.keyfigures,
            "date": self.calc_date.strftime("%Y-%m-%d"),
        }

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
        symbol: List[str],
        keyfigures: Union[
            List[str], List[BondKeyFigureName], List[Union[str, BondKeyFigureName]]
        ],
        from_date: datetime,
        to_date: datetime,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientFile for testing.
            symbol: Bonds ISINs, swaps, FX, FX swap point.
            keyfigures: Key figure names for request. If symbol is
                something else than a bond ISIN, quote should be chosen.
            from_date: From date for calc date interval.
            to_date: To date for calc date interval.
        """
        super(TimeSeries, self).__init__(client)
        self._client = client
        self.symbol = ",".join(symbol) if len(symbol) > 1 else symbol
        self.keyfigures = [
            convert_to_variable_string(keyfigure, BondKeyFigureName)
            for keyfigure in keyfigures
        ]
        self.from_date = from_date
        self.to_date = to_date
        self._data = self.get_time_series()

    def get_time_series(self) -> Dict:
        """Retrieves response with key figures time series."""
        json_response = self.get_response()
        return json_response["response"]

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return "historical/timeseries/"

    @property
    def request(self) -> Dict:
        """Request dictionary time series key figures."""
        return {
            "symbols": self.symbol,
            "keyFigures": self.keyfigures,
            "from": self.from_date.strftime("%Y-%m-%d"),
            "to": self.to_date.strftime("%Y-%m-%d"),
        }

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {}
        for isin_data in self._data:
            _timeseries_dict: Dict[Any, Any] = {}
            for timeseries in isin_data["timeSeries"]:
                key_figure_name = BondKeyFigureName(timeseries["keyfigure"]).name
                _timeseries_dict[key_figure_name] = {}
                _timeseries_dict[key_figure_name]["Date"] = [
                    datetime.strptime(x["key"], "%Y-%m-%dT%H:%M:%S.0000000")
                    for x in timeseries["values"]
                ]
                _timeseries_dict[key_figure_name]["Value"] = [
                    convert_to_float_if_float(x["value"]) for x in timeseries["values"]
                ]

            _dict[isin_data["symbol"]] = _timeseries_dict

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        df = pd.DataFrame.empty
        _dict = self.to_dict()
        for isin in _dict:
            _df = pd.DataFrame.empty
            for keyfigure in _dict[isin]:
                _df_keyfigure = pd.DataFrame.from_dict(_dict[isin][keyfigure])
                _df_keyfigure = _df_keyfigure[["Date", "Value"]]
                _df_keyfigure.columns = ["Date", keyfigure]
                if _df is pd.DataFrame.empty:
                    _df = _df_keyfigure
                else:
                    _df = _df.merge(_df_keyfigure, on="Date", how="outer")
            _df = _df.sort_values(by="Date")
            _df.insert(0, "ISIN", [isin] * len(_df))

            if df is pd.DataFrame.empty:
                df = _df
            else:
                df = df.append(_df)
        return df


class IndexComposition(ValueRetriever):
    """Retrieves and reformat index composition for a given indices and calc date."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        indices: List[str],
        calc_date: datetime,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientFile for testing.
            indices: Indices for request.
            calc_date: calculation date for request.
        """
        super(IndexComposition, self).__init__(client)
        self._client = client
        self.indices = indices
        self.calc_date = calc_date
        self._data = self.get_index_composition()

    def get_index_composition(self) -> Dict:
        """Calls the client and retrieves response with index comp. from service."""
        json_response = self.get_response()
        return json_response["underlyings"]

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return "indices/"

    @property
    def request(self) -> Dict:
        """Request dictionary for a given set of indices and calc date."""
        return {
            "symbols": self.indices,
            "infodate": self.calc_date.strftime("%Y-%m-%d"),
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
            _dict[index_data["indexName"]["name"]] = _isin_dict

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
    """Retrieves and reformat curvetime series."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        curve: str,
        from_date: datetime,
        to_date: datetime,
        curve_type: Union[str, CurveType],
        time_convention: Union[str, TimeConvention],
        tenors: List[float],
        spot_forward: Optional[Union[str, SpotForward]],
        forward_tenor: Optional[float] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientFile for testing.
            curve: Name of curve that should be retrieved.
            from_date: From date for calc date interval.
            to_date: To date for calc date interval.
            curve_type: What type of curve is retrieved.
            time_convention: Time convention used when curve is constructed.
            tenors: For what tenors should be curve be constructed.
            spot_forward: Should the curve be spot, spot forward
                 or implied forward.
            forward_tenor: Forward tenor for forward curve or implied forward curve.
        """
        super(CurveTimeSeries, self).__init__(client)
        self._client = client
        self.curve = curve
        self.from_date = from_date
        self.to_date = to_date
        self.curve_type = convert_to_variable_string(curve_type, CurveType)
        self.time_convention = convert_to_variable_string(
            time_convention, TimeConvention
        )
        self.tenors = [str(t) for t in tenors]
        self.spot_forward = (
            convert_to_variable_string(spot_forward, SpotForward)
            if spot_forward is not None
            else None
        )
        self.forward_tenor = self.check_forward(forward_tenor)

        self._data = self.get_curve_time_series()

    def get_curve_time_series(self) -> Dict:
        """Retrieves response with curve time series."""
        json_response = self.get_response()
        return json_response["result"]

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
        return "historical/curves/"

    @property
    def request(self) -> Dict:
        """Request dictionary curve time series."""
        request = {
            "from": self.from_date.strftime("%Y-%m-%d"),
            "to": self.to_date.strftime("%Y-%m-%d"),
            "curve": self.curve,
            "type": self.curve_type,
            "timeconvention": self.time_convention,
            "tenors": self.tenors,
        }
        if self.spot_forward is not None:
            request["SpotForward"] = self.spot_forward

        if self.forward_tenor is not None:
            request["Forward"] = self.forward_tenor

        return request

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict: Dict[Any, Any] = {}
        for timeseries in self._data["dates"]:
            for tenor in timeseries["values"]:
                if self.forward_tenor is None:
                    curve_and_tenor = self.curve + "(" + str(tenor["tenor"]) + "Y)"
                else:
                    curve_and_tenor = (
                        self.curve
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
                        datetime.strptime(
                            timeseries["date"], "%Y-%m-%dT%H:%M:%S.0000000"
                        )
                    ]
                else:
                    _dict[curve_and_tenor]["Value"].append(
                        convert_to_float_if_float(tenor["value"])
                    )
                    _dict[curve_and_tenor]["Date"].append(
                        datetime.strptime(
                            timeseries["date"], "%Y-%m-%dT%H:%M:%S.0000000"
                        )
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
