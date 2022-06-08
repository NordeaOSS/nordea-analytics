from datetime import datetime
import math
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd

from nordea_analytics.key_figure_names import (
    TimeSeriesKeyFigureName,
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
            json_map = _json_response[config["results"]["time_series"]]
            json_response = list(json_map) + json_response

        output_found = check_json_response(json_response)
        check_json_response_error(output_found)

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
                key_figure_name = TimeSeriesKeyFigureName(timeseries["keyfigure"]).name
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
