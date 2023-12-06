from typing import Dict, Any

from nordea_analytics.nalib.http.core import RestApiHttpClient
from nordea_analytics.nalib.http.models import AnalyticsApiResponse
from nordea_analytics.nalib.util import RequestMethod


class BaseDataRetrievalClient:
    """A base client for making API requests to the Nordea Analytics REST API and handling responses."""

    def __init__(self, http_client: RestApiHttpClient) -> None:
        """Constructs a :class:`BaseDataRetrievalClient <BaseDataRetrievalClient>`.

        Args:
            http_client: The HTTP client used to make requests.
        """
        self.__http_client = http_client

    @property
    def http_client(self) -> RestApiHttpClient:
        """Return http client."""
        return self.__http_client

    def send(
        self,
        request: Dict,
        url_suffix: str,
        request_method: RequestMethod,
        **kwargs: Any,
    ) -> AnalyticsApiResponse:
        """Sends a request to the API and returns the response.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method
            request_method: Request method type
            **kwargs: parameters for HTTP Request.

        Returns:
            AnalyticsApiResponse response.

        Raises:
            ValueError: if request different from GET or POST
        """
        if request_method == RequestMethod.Get:
            return self.http_client.get(url_suffix=url_suffix, params=request, **kwargs)

        if request_method == RequestMethod.Post:
            return self.http_client.post(url_suffix=url_suffix, json=request, **kwargs)

        raise ValueError(f"Invalid request method: {request_method}")
