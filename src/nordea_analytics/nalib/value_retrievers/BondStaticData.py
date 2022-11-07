from datetime import datetime
import math
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class BondStaticData(ValueRetriever):
    """Retrieves and reformat latest static data for given bonds."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        symbols: Union[List[str], str],
    ) -> None:
        """Initialization of class.

        Args:
            client:  DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing
            symbols: ISIN or name of bonds for requests.
        """
        super(BondStaticData, self).__init__(client)

        self.symbols: List = [symbols] if type(symbols) != list else symbols
        self._data = self.get_bond_static_data()

    def get_bond_static_data(self) -> List:
        """Calls the client and retrieves response with static data from the service."""
        json_response: List[Any] = []
        for request_dict in self.request:
            _json_response = self.get_response(request_dict)
            json_map = _json_response[config["results"]["bond_static_data"]]
            json_response = list(json_map) + json_response

        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["bond_static_data"]

    @property
    def request(self) -> List[Dict]:
        """Request dictionary for a given set of symbols."""
        if len(self.symbols) > config["max_bonds"]:
            split_symbols = np.array_split(
                self.symbols, math.ceil(len(self.symbols) / config["max_bonds"])
            )
            request_dict = [
                {
                    "symbols": list(symbols),
                }
                for symbols in split_symbols
            ]
        else:
            request_dict = [
                {
                    "symbols": self.symbols,
                }
            ]

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {}
        for bond_data in self._data:
            _symbol_dict = {}
            _symbol_dict["name"] = bond_data["name"]

            for static_data_key in bond_data["static_data"]:
                key_value_pair = bond_data["static_data"][static_data_key]

                if (
                    key_value_pair["key"] == "closing_date"
                    or key_value_pair["key"] == "issue_date"
                    or key_value_pair["key"] == "maturity"
                    or key_value_pair["key"] == "retrieval_date"
                ):
                    _symbol_dict[key_value_pair["key"]] = datetime.strptime(
                        key_value_pair["value"], "%Y-%m-%dT%H:%M:%S.0000000"
                    )
                else:
                    _symbol_dict[key_value_pair["key"]] = key_value_pair["value"]

            _dict[bond_data["symbol"]] = _symbol_dict

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        return pd.DataFrame.from_dict(self.to_dict(), orient="index")
