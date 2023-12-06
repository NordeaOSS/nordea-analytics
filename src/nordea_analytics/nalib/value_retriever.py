from abc import ABC, abstractmethod
from typing import Dict, List, Union

import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import DataRetrievalServiceClient
from nordea_analytics.nalib.util import get_config

config = get_config()


class ValueRetriever(ABC):
    """Base class for retrieving values from the DataRetrievalServiceClient.

    This class provides a common interface for retrieving values from a service
    using the DataRetrievalServiceClient. Subclasses of this base class can
    implement specific methods to retrieve data from the service and process
    the response into desired formats.

    Attributes:
        _client (DataRetrievalServiceClient): The client object used to interact
            with the Data Retrieval Service.

    Methods:
        get_response(request: Dict) -> Dict:
            Calls the DataRetrievalServiceClient to get a response from the service.

        url_suffix (property):
            Abstract property that defines the URL suffix for a given method.

        request (property):
            Abstract property that defines the request dictionary for a given method.

        to_dict() -> Dict:
            Abstract method that reformats the JSON response to a dictionary.

        to_df() -> pd.DataFrame:
            Abstract method that reformats the JSON response to a pandas DataFrame.
    """

    def __init__(self, client: DataRetrievalServiceClient) -> None:
        """Initialize the ValueRetriever with a DataRetrievalServiceClient.

        Args:
            client (DataRetrievalServiceClient): The client object used to interact
                with the Data Retrieval Service.
        """
        self._client = client

    def get_response(self, request: Dict) -> Dict:
        """Call the DataRetrievalServiceClient to get a response from the service.

        Args:
            request (Dict): The request dictionary.

        Returns:
            Dict: The response from the service for a given method and request.
        """
        json_response = self._client.get(request, self.url_suffix)
        return json_response

    @property
    @abstractmethod
    def url_suffix(self) -> str:
        """URL suffix for a given method.

        This is an abstract property that should be implemented by subclasses
        to define the URL suffix for the specific method being used to retrieve data
        from the service.

        Returns:
            str: The URL suffix for the given method.
        """
        pass

    @property
    @abstractmethod
    def request(self) -> Union[Dict, List[Dict]]:
        """Request dictionary for a given method.

        This is an abstract property that should be implemented by subclasses
        to define the request dictionary for the specific method being used to retrieve data
        from the service.

        Returns:
            Union[Dict, List[Dict]]: The request dictionary for the given method.
        """
        pass

    @abstractmethod
    def to_dict(self) -> Dict:
        """Reformat the JSON response to a dictionary.

        This is an abstract method that should be implemented by subclasses
        to process the JSON response from the service and reformat it into a dictionary
        for further processing.

        Returns:
            Dict: The reformatted dictionary from the JSON response.
        """
        pass

    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        """Reformat the JSON response to a pandas DataFrame.

        This is an abstract method that should be implemented by subclasses
        to process the JSON response from the service and reformat it into a pandas DataFrame
        for further processing.

        Returns:
            pd.DataFrame: The reformatted DataFrame from the JSON response.
        """
        pass
