from typing import Any, Dict, List

import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import get_config
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class InstrumentSearch(ValueRetriever):
    """Searches available instruments by name or symbol.

    Args:
        client: The client used to retrieve data.
        text: Text to be searched for.
        exact_match: If true, then either symbol or name of instrument must be exactly like provided text.
        instrument_group_ids: Search only inside selected instrument groups.
        search_descendant_groups: If any instrument group is selected, then descendant groups are also used for search.
    """

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        text: str,
        exact_match: bool,
        instrument_group_ids: List[int],
        search_descendant_groups: bool,
    ) -> None:
        """Initialization of the InstrumentSearch class."""
        super(InstrumentSearch, self).__init__(client)
        self._text = text
        self._exact_match = exact_match
        self._instrument_group_ids = instrument_group_ids
        self._search_descendant_groups = search_descendant_groups
        self._instruments = self.get_instruments()

    def get_instruments(self) -> Dict:
        """Retrieves response with key figures time series.

        Returns:
            List of JSON response with key figures time series.
        """

        dict_request = self.request
        json_response = self.get_response(dict_request)
        property = config["results"]["instrument_search"]

        return json_response[property]

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method.

        Returns:
            Url suffix for the instrument search.
        """
        suffix = config["url_suffix"]["search_instruments"]
        return suffix

    @property
    def request(self) -> Dict:
        """Request dictionary for instrument search.

        Returns:
            List of request dictionary.
        """

        # Generate request dictionary
        request_dict = {"text": self._text, "exact-match": self._exact_match}

        if len(self._instrument_group_ids) > 0:
            request_dict["instrument-group-ids"] = self._instrument_group_ids
            request_dict["search-descendant-groups"] = self._search_descendant_groups

        return request_dict

    def to_dict(self) -> dict:
        """Convert the JSON response to a dictionary.

        Returns:
            The converted dictionary.
        """
        dict: Dict[Any, Any] = {}
        for i in range(len(self._instruments)):
            dict[i] = {}
            dict[i]["Symbol"] = self._instruments[i]["symbol"]
            dict[i]["Name"] = self._instruments[i]["name"]
            dict[i]["Is_curve"] = bool(self._instruments[i]["is_curve"])
            if "instrument_group" in self._instruments[i]:
                dict[i]["Instrument_group"] = int(
                    self._instruments[i]["instrument_group"]
                )
            if "instrument_group_name" in self._instruments[i]:
                dict[i]["Instrument_group_name"] = self._instruments[i][
                    "instrument_group_name"
                ]

        return dict

    def to_df(self) -> pd.DataFrame:
        """Convert the JSON response to a pandas DataFrame.

        Returns:
            The converted pandas DataFrame.
        """
        _dict = self.to_dict()  # Convert JSON response to dictionary
        df = pd.DataFrame.from_dict(_dict)
        return df
