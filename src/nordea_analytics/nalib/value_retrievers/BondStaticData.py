from datetime import datetime
import math
from typing import Any, Dict, List, Mapping, Union

import numpy as np
import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    check_json_response,
    check_json_response_error,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class BondStaticData(ValueRetriever):
    """Retrieves and reformat latest bond static data for given ISINs."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        isins: Union[List[str], str],
    ) -> None:
        """Initialization of class.

        Args:
            client:  DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing
            isins: ISINs for requests.
        """
        super(BondStaticData, self).__init__(client)

        self.isins: List = [isins] if type(isins) != list else isins
        self._data = self.get_bond_static_data()

    def get_bond_static_data(self) -> List:
        """Calls the client and retrieves response with static data from the service."""
        json_response: List[Any] = []
        for request_dict in self.request:
            _json_response = self.get_response(request_dict)
            json_map = _json_response[config["results"]["bond_static_data"]]
            json_response = list(json_map) + json_response

        self.check_response(json_response)

        return json_response

    @staticmethod
    def check_response(json_response: Union[List, Mapping]) -> None:
        """Checks if json_reponse contains output, else throws error."""
        output_found = False
        for i in range(0, json_response.__len__()):
            output_found = check_json_response(json_response[i]["static_data"])
            if output_found:
                break

        check_json_response_error(output_found)

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["bond_static_data"]

    @property
    def request(self) -> List[Dict]:
        """Request dictionary for a given set of ISINs."""
        if len(self.isins) > config["max_isins"]:
            split_isins = np.array_split(
                self.isins, math.ceil(len(self.isins) / config["max_isins"])
            )
            request_dict = [
                {
                    "symbols": list(isins),
                }
                for isins in split_isins
            ]
        else:
            request_dict = [
                {
                    "symbols": self.isins,
                }
            ]

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {}
        for isin_data in self._data:
            _isin_dict = {}
            _isin_dict["name"] = isin_data["name"]

            for static_data_key in isin_data["static_data"]:
                key_value_pair = isin_data["static_data"][static_data_key]

                if (
                    key_value_pair["key"] == "closing_date"
                    or key_value_pair["key"] == "issue_date"
                    or key_value_pair["key"] == "maturity"
                    or key_value_pair["key"] == "retrieval_date"
                ):
                    _isin_dict[key_value_pair["key"]] = datetime.strptime(
                        key_value_pair["value"], "%Y-%m-%dT%H:%M:%S.0000000"
                    )
                else:
                    _isin_dict[key_value_pair["key"]] = key_value_pair["value"]

            _dict[isin_data["symbol"]] = _isin_dict

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        return pd.DataFrame.from_dict(self.to_dict(), orient="index")
