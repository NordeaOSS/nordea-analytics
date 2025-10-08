from typing import Dict, List

from nordea_analytics.nalib.background_requests.core import BackgroundRequestsClient
from nordea_analytics.nalib.data_retrieval_client import validation
from nordea_analytics.nalib.data_retrieval_client.core import BaseDataRetrievalClient
from nordea_analytics.nalib.http.core import RestApiHttpClient
from nordea_analytics.nalib.live_keyfigures.core import HttpStreamIterator
from nordea_analytics.nalib.util import RequestMethod


class DataRetrievalServiceClient(BaseDataRetrievalClient):
    """A client for making API requests to the Nordea Analytics REST API and handling responses."""

    def __init__(
        self,
        http_client: RestApiHttpClient,
        stream_listener: HttpStreamIterator,
        background_client: BackgroundRequestsClient,
    ) -> None:
        """Constructs a :class:`DataRetrievalServiceClient <DataRetrievalServiceClient>`.

        Args:
            http_client: The HTTP client used to make requests.
            stream_listener: Iterator for consuming Server Events streams.
            background_client: Background client for send/get background requests.
        """
        super().__init__(http_client)
        self.__stream_listener = stream_listener
        self.__background_client = background_client

    @property
    def diagnostic(self) -> List:
        """Return diagnostic information about the last request."""
        if self.http_client.history:
            last = self.http_client.history[-1]
            response_chain: List = list()
            for response in reversed(self.http_client.history):
                if response.request_id != last.request_id:
                    break

                response_chain.insert(0, response.diagnostic)
            return response_chain

        return []

    def get_live_streamer(self) -> HttpStreamIterator:
        """Method return HttpStreamIterator which allow iteration over stream."""
        return self.__stream_listener

    def request_calculation(self, request: Dict, url_suffix: str) -> List:
        """Sends a calculation request and retrieves the response."""
        return self.__background_client.get_calculation_asynchronous(
            request, url_suffix
        )

    def get_response_asynchronous(self, request: Dict, url_suffix: str) -> Dict:
        """Sends the asynchronous request and retrieves the response."""
        return self.__background_client.retrieve_response_asynchronous(
            request, url_suffix, "GET"
        )

    def post_response_asynchronous(self, request: Dict, url_suffix: str) -> Dict:
        """Sends the asynchronous request and retrieves the response."""
        return self.__background_client.retrieve_response_asynchronous(
            request, url_suffix, "POST"
        )

    def get(self, request: Dict, url_suffix: str) -> Dict:
        """Sends a GET request to the API and returns the response.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
            Response in the form of Dict.
        """
        return self.__retrieve(request, url_suffix, RequestMethod.Get)

    def post(self, request: Dict, url_suffix: str) -> Dict:
        """Sends a POST request to the API and returns the response.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
            Response in the form of Dict.
        """
        return self.__retrieve(request, url_suffix, RequestMethod.Post)

    def __retrieve(
        self,
        request: Dict,
        url_suffix: str,
        request_method: RequestMethod = RequestMethod.Get,
    ) -> Dict:
        api_response = self.send(request, url_suffix, request_method)
        validation.validate_response(api_response)
        return api_response.data  # type: ignore
