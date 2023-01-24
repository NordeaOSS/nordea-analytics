from typing import Dict

import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import DataRetrievalServiceClient
from nordea_analytics.nalib.util import (
    get_config,
    RequestMethod,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class LiveBondUniverse(ValueRetriever):
    """Retrieves supported live bond universe."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
    ) -> None:
        """Initialization of class.

        Args:
            client: LiveDataRetrivalServiceClient
        """
        super(LiveBondUniverse, self).__init__(client)

        self._data = self.get_live_bond_universe_response

    @property
    def get_live_bond_universe_response(self) -> Dict:
        """Returns the latest available live key figures from cache."""
        json_response = self._client.get_response(
            self.request, self.url_suffix, RequestMethod.Get
        )

        json_response.pop("count")
        json_response.pop("restricted")

        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["live_bond_universe"]

    @property
    def request(self) -> Dict:
        """Request should be empty, endpoint does not have any inputs."""
        return {}

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        return self._data

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()
        df = pd.DataFrame({k: pd.Series(v) for k, v in _dict.items()})

        return df
