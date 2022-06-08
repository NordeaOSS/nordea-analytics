from datetime import datetime
from typing import Dict, List, Mapping, Union

import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    check_json_response,
    check_json_response_error,
    convert_to_float_if_float,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


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
        json_response = json_response[config["results"]["index_composition"]]

        output_found = check_json_response(json_response)
        check_json_response_error(output_found)

        return json_response

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
