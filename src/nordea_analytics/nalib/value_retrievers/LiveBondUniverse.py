from typing import Dict

import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import DataRetrievalServiceClient
from nordea_analytics.nalib.exceptions import CustomWarningCheck
from nordea_analytics.nalib.util import get_config
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class LiveBondUniverse(ValueRetriever):
    """Retrieves the supported live bond universe.

    This class inherits from ValueRetriever and provides methods to retrieve and reformat data
    from the live bond universe using a DataRetrievalServiceClient instance.
    """

    def __init__(self, client: DataRetrievalServiceClient) -> None:
        """Initializes the LiveBondUniverse instance.

        Args:
            client: The client used to retrieve data.
        """
        super(LiveBondUniverse, self).__init__(client)
        self._data = self.get_live_bond_universe_response

    @property
    def get_live_bond_universe_response(self) -> Dict:
        """Returns the latest available live key figures from the cache.

        Returns:
            A dictionary containing the latest available live key figures.
        """
        json_response = self._client.get(self.request, self.url_suffix)

        # Remove unnecessary keys from the response
        json_response.pop("count")
        json_response.pop("restricted")

        if "errors" in json_response:
            CustomWarningCheck.live_key_figure_universe_warning(response=json_response)
            json_response.pop("errors")

        return json_response

    @property
    def url_suffix(self) -> str:
        """Returns the URL suffix for the live bond universe.

        Returns:
            The URL suffix for the live bond universe.
        """
        return config["url_suffix"]["live_bond_universe"]

    @property
    def request(self) -> Dict:
        """Returns an empty request, as the live bond universe endpoint does not have any inputs.

        Returns:
            An empty dictionary representing the request.
        """
        return {}

    def to_dict(self) -> Dict:
        """Reformats the JSON response to a dictionary.

        Returns:
            A dictionary containing the reformatted data from the JSON response.
        """
        return self._data

    def to_df(self) -> pd.DataFrame:
        """Reformats the JSON response to a pandas DataFrame.

        Returns:
            A pandas DataFrame containing the reformatted data from the JSON response.
        """
        _dict = self.to_dict()
        df = pd.DataFrame({k: pd.Series(v) for k, v in _dict.items()})

        return df
