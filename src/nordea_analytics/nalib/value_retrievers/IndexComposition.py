from datetime import datetime
from typing import Dict, List, Mapping, Union

import pandas as pd

from nordea_analytics.instrument_variable_names import BondIndexName
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    convert_to_list,
    convert_to_float_if_float,
    convert_to_original_format,
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class IndexComposition(ValueRetriever):
    """Retrieves and reformats index composition for a given set of indices and calculation date."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        indices: Union[
            str,
            BondIndexName,
            List[str],
            List[BondIndexName],
            List[Union[str, BondIndexName]],
            pd.Series,
            pd.Index,
        ],
        calc_date: datetime,
    ) -> None:
        """Initialize the class.

        Args:
            client: The client used to retrieve data.
            indices:
                List of indices or a single index for which to retrieve the composition.
            calc_date: Calculation date for the index composition.
        """
        super(IndexComposition, self).__init__(client)
        self._client = client

        self.indices_original = convert_to_list(indices)

        # Convert index names to variable strings
        _indices: List = []
        for index in self.indices_original:
            if isinstance(index, BondIndexName):
                _indices.append(convert_to_variable_string(index, BondIndexName))
            else:
                _indices.append(index)
        self.indices = _indices

        self.calc_date = calc_date
        self._data = self.get_index_composition()

    def get_index_composition(self) -> Mapping:
        """Calls the client and retrieves response with index composition from the service.

        Returns:
            Dictionary containing the index composition data.
        """
        json_response = self.get_response(self.request)
        json_response = json_response[config["results"]["index_composition"]]

        return json_response

    @property
    def url_suffix(self) -> str:
        """URL suffix for the index composition endpoint.

        Returns:
            String containing the URL suffix for the index composition endpoint.
        """
        return config["url_suffix"]["index_composition"]

    @property
    def request(self) -> Dict:
        """Request dictionary for a given set of indices and calculation date.

        Returns:
            Dictionary containing the request parameters for retrieving index composition.
        """
        return {
            "symbols": self.indices,
            "date": self.calc_date.strftime("%Y-%m-%d"),
        }

    def to_dict(self) -> Dict:
        """Reformat the JSON response to a dictionary.

        Returns:
            Dictionary containing the reformatted index composition data.
        """
        _dict = {}
        for index_data in self._data:
            _index_dict = {}
            _index_dict["ISIN"] = [x["symbol"] for x in index_data["underlyings"]]
            _index_dict["Name"] = [x["name"] for x in index_data["underlyings"]]
            _index_dict["Nominal_Amount"] = [
                convert_to_float_if_float(x["nominal"])
                for x in index_data["underlyings"]
            ]
            sum_nominal = sum(_index_dict["Nominal_Amount"])
            _index_dict["Nominal_Weight"] = [
                x / sum_nominal for x in _index_dict["Nominal_Amount"]
            ]

            _index_dict["Market_Amount"] = [
                convert_to_float_if_float(x["market"]) if "market" in x else None
                for x in index_data["underlyings"]
            ]

            if not _index_dict["Market_Amount"].__contains__(None):
                sum_market = sum(_index_dict["Market_Amount"])
                _index_dict["Market_Weight"] = [
                    x / sum_market for x in _index_dict["Market_Amount"]
                ]
            index_original = convert_to_original_format(
                index_data["index_name"]["name"], self.indices_original
            )
            _dict[index_original] = _index_dict

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the JSON response to a pandas DataFrame.

        Returns:
            Pandas DataFrame containing the reformatted index composition data.
        """
        df = pd.DataFrame()
        _dict = self.to_dict()
        for index in _dict:
            _df = pd.DataFrame.from_dict(_dict[index])
            _df.insert(0, "Index", [index] * len(_df))

            if df.empty:
                df = _df
            else:
                df = pd.concat([df, _df], axis=0)
        return df
