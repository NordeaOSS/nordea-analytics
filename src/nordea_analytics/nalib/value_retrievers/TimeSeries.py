from datetime import datetime, timedelta
import math
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd

from nordea_analytics.instrument_variable_names import BenchmarkName, BondIndexName
from nordea_analytics.key_figure_names import (
    TimeSeriesKeyFigureName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    convert_to_float_if_float,
    convert_to_original_format,
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
        symbols: Union[
            str,
            BondIndexName,
            BenchmarkName,
            List[str],
            List[BenchmarkName],
            List[BondIndexName],
            List[Union[str, BenchmarkName, BondIndexName]],
        ],
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
            symbols: Bonds, swaps, FX, FX swap point.
            keyfigures: Key figure names for request. If symbol is
                something else than a bonds, quote should be chosen.
            from_date: From date for calc date interval.
            to_date: To date for calc date interval.
        """
        super(TimeSeries, self).__init__(client)
        self._client = client
        self.symbols_original: List = (
            symbols if isinstance(symbols, list) else [symbols]
        )

        _symbols: List = []
        for symbol in self.symbols_original:
            if isinstance(symbol, BenchmarkName):
                _symbols.append(convert_to_variable_string(symbol, BenchmarkName))
            elif isinstance(symbol, BondIndexName):
                _symbols.append(convert_to_variable_string(symbol, BondIndexName))
            else:
                _symbols.append(symbol)
        self.symbols = _symbols

        self.keyfigures_original: List = (
            keyfigures if isinstance(keyfigures, list) else [keyfigures]
        )
        self.keyfigures = [
            convert_to_variable_string(keyfigure, TimeSeriesKeyFigureName)
            if type(keyfigure) == TimeSeriesKeyFigureName
            else keyfigure
            for keyfigure in self.keyfigures_original
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

        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method."""
        return config["url_suffix"]["time_series"]

    @property
    def request(self) -> List[Dict]:
        """Request dictionary time series key figures."""
        intv = config["max_years_timeseries"] * 365
        date_interv = []
        new_from_date = self.from_date
        while (self.to_date - new_from_date).days > intv:
            new_to_date = new_from_date + timedelta(days=intv)
            date_interv.append({"from": new_from_date, "to": new_to_date})
            new_from_date = new_from_date + timedelta(days=intv + 1)
            if new_from_date > self.to_date:
                new_from_date = self.to_date

        date_interv.append({"from": new_from_date, "to": self.to_date})

        split_symbol = np.array_split(
            self.symbols, math.ceil(len(self.symbols) / config["max_symbol_timeseries"])
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
        for symbol_data in self._data:
            _timeseries_dict: Dict[Any, Any] = {}
            symbol_original = convert_to_original_format(
                symbol_data["symbol"], self.symbols_original
            )
            for timeseries in symbol_data["timeseries"]:
                key_figure_original = convert_to_original_format(
                    timeseries["keyfigure"], self.keyfigures_original
                )
                _timeseries_dict[key_figure_original] = {}
                _timeseries_dict[key_figure_original]["Date"] = [
                    datetime.strptime(x["key"], "%Y-%m-%d")
                    for x in timeseries["values"]
                ]
                _timeseries_dict[key_figure_original]["Value"] = [
                    convert_to_float_if_float(x["value"]) for x in timeseries["values"]
                ]

                if symbol_data["symbol"] in _dict.keys():
                    if key_figure_original in _dict[symbol_data["symbol"]].keys():
                        if (
                            _dict[symbol_original][key_figure_original]["Date"][-1]
                            > _timeseries_dict[key_figure_original]["Date"][0]
                        ):
                            _dict[symbol_original][key_figure_original][
                                "Date"
                            ] += _timeseries_dict[key_figure_original]["Date"]
                            _dict[symbol_original][key_figure_original][
                                "Value"
                            ] += _timeseries_dict[key_figure_original]["Value"]
                        else:
                            _dict[symbol_original][key_figure_original]["Date"] = (
                                _timeseries_dict[key_figure_original]["Date"]
                                + _dict[symbol_original][key_figure_original]["Date"]
                            )
                            _dict[symbol_original][key_figure_original]["Value"] = (
                                _timeseries_dict[key_figure_original]["Value"]
                                + _dict[symbol_original][key_figure_original]["Value"]
                            )
                    else:
                        _dict[symbol_original].update(_timeseries_dict)
                else:
                    _dict[symbol_original] = _timeseries_dict

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        df = pd.DataFrame()
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

            if df.empty:
                df = _df
            else:
                df = pd.concat([df, _df], axis=0)
        return df.reset_index(drop=True)
