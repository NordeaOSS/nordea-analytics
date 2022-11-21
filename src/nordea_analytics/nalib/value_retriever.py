from abc import ABC, abstractmethod
from typing import Dict, List, Union

import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    get_config,
    RequestMethod,
)

config = get_config()


class ValueRetriever(ABC):
    """Base class for retrieving values from the DataRetrievalServiceClient."""

    def __init__(self, client: DataRetrievalServiceClient) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
        """
        self._client = client

    def get_response(self, request: Dict) -> Dict:
        """Calls the DataRetrievalServiceClient to get a response from the service.

        Args:
            request: request dictionary.

        Returns:
            Response from the service for a given method and request.
        """
        json_response = self._client.get_response(
            request, self.url_suffix, RequestMethod.Get
        )
        return json_response

    @property
    @abstractmethod
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        pass

    @property
    @abstractmethod
    def request(self) -> Union[Dict, List[Dict]]:
        """Creates a request dictionary for a given method."""
        pass

    @abstractmethod
    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        pass

    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        pass
