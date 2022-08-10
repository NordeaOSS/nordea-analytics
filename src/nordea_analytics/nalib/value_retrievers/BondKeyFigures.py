from datetime import datetime
import math
from typing import Any, Dict, List, Mapping, Union

import numpy as np
import pandas as pd

from nordea_analytics.key_figure_names import (
    BondKeyFigureName,
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


class BondKeyFigures(ValueRetriever):
    """Retrieves and reformat given bond key figures for given bonds and calc date."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        symbols: Union[List[str], str],
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
            symbols: ISIN or name of bonds for requests.
            keyfigures: Bond key figure names for request.
            calc_date: calculation date for request.
        """
        super(BondKeyFigures, self).__init__(client)

        self.symbols: List = [symbols] if type(symbols) != list else symbols
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
        json_failed_queries: str
        for request_dict in self.request:
            _json_response = self.get_response(request_dict)
            json_map = _json_response[config["results"]["bond_key_figures"]]
            json_response = list(json_map) + json_response

        self.check_response(json_response)

        return json_response

    @staticmethod
    def check_response(json_response: Union[List, Mapping]) -> None:
        """Checks if json_reponse contains output, else throws error."""
        output_found = False
        for i in range(0, json_response.__len__()):
            output_found = check_json_response(json_response[i]["values"])
            if output_found:
                break

        check_json_response_error(output_found)

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["bond_key_figures"]

    @property
    def request(self) -> List[Dict]:
        """Request dictionary for a given set of symbols, key figures and calc date."""
        if len(self.symbols) > config["max_bonds"]:
            split_symbols = np.array_split(
                self.symbols, math.ceil(len(self.symbols) / config["max_bonds"])
            )
            request_dict = [
                {
                    "symbols": list(symbols),
                    "keyfigures": self.keyfigures,
                    "date": self.calc_date.strftime("%Y-%m-%d"),
                }
                for symbols in split_symbols
            ]
        else:
            request_dict = [
                {
                    "symbols": self.symbols,
                    "keyFigures": self.keyfigures,
                    "date": self.calc_date.strftime("%Y-%m-%d"),
                }
            ]

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {}
        for bond_data in self._data:
            _bond_dict = {}
            for key_figure_data in bond_data["values"]:
                key_figure_name = BondKeyFigureName(key_figure_data["keyfigure"]).name
                _bond_dict[key_figure_name] = convert_to_float_if_float(
                    key_figure_data["value"]
                )

            _dict[bond_data["symbol"]] = _bond_dict

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        return pd.DataFrame.from_dict(self.to_dict(), orient="index")
