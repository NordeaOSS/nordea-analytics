from typing import Dict, List

from nordea_analytics.nalib.data_retrieval_client import validation
from nordea_analytics.nalib.data_retrieval_client.background import (
    BackgroundRequestsClient,
)
from nordea_analytics.nalib.http.core import RestApiHttpClient
from nordea_analytics.nalib.live_keyfigures.core import HttpStreamIterator
from nordea_analytics.nalib.util import RequestMethod


class DataRetrievalServiceClient(BackgroundRequestsClient):
    """A client for making API requests to the Nordea Analytics REST API and handling responses."""

    def __init__(
        self, http_client: RestApiHttpClient, stream_listener: HttpStreamIterator
    ) -> None:
        """Constructs a :class:`DataRetrievalServiceClient <DataRetrievalServiceClient>`.

        Args:
            http_client: The HTTP client used to make requests.
            stream_listener: Iterator for consuming Server Events streams.
        """
        super(BackgroundRequestsClient, self).__init__(http_client)
        self.__stream_listener = stream_listener

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
        if not self._is_background_response(api_response):
            validation.validate_response(api_response)
            return api_response.data  # type: ignore

        # Process background job
        api_response = self._poll_server(api_response)
        validation.validate_response(api_response)
        return api_response.data_response  # type: ignore
