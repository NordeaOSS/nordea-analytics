from typing import Any, Dict, Union

from nordea_analytics.nalib.exceptions import HttpClientImproperlyConfigured
from nordea_analytics.nalib.http.core import HttpClientConfiguration, RestApiHttpClient
from nordea_analytics.nalib.http.errors import ClientHttpError, ForbiddenRequestError
from nordea_analytics.nalib.http.models import AnalyticsApiResponse


class OpenBankingForbiddenRequestError(ForbiddenRequestError):
    """Analytics API server is refusing action due to insufficient privileges."""

    def __init__(self, error_id: str) -> None:
        """Create a new instance of OpenBankingUnauthorizedRequestError."""
        self._error_description = (
            "Your client_id and client_secret are correct, "
            "but you don't have access to requested resource. "
            "Please contact Nordea to get access."
        )
        super().__init__(error_id, self._error_description)

    def __str__(self) -> str:
        """Return str(self)."""
        return f"Error code: {self.error_id}. {self._error_description}"


class OpenBankingUnauthorizedRequestError(ClientHttpError):
    """Authentication has failed.

    Check that you pass correct client_id and client_secret values and that you have proper access to Analytics API.
    """

    def __init__(self, error_id: str) -> None:
        """Create a new instance of OpenBankingUnauthorizedRequestError."""
        self._error_description = (
            "Authentication has failed. "
            "Check that you pass correct client_id and client_secret values "
            "and that you have proper access to Analytics API."
        )
        super().__init__(error_id, self._error_description)

    def __str__(self) -> str:
        """Return str(self)."""
        return self._error_description


class OpenBankingClientConfiguration(HttpClientConfiguration):
    """Contain parameters for HTTP requests to open banking API."""

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        headers: Union[Dict[str, str], None] = None,
        proxies: Union[Dict[str, str], None] = None,
    ) -> None:
        """Constructs a :class:`OpenBankingClientConfiguration <OpenBankingClientConfiguration>`."""
        if not base_url:
            raise HttpClientImproperlyConfigured("base_url is not set.")
        if not client_id:
            raise HttpClientImproperlyConfigured("client_id is not set.")
        if not client_secret:
            raise HttpClientImproperlyConfigured("client_secret is not set.")

        self.__client_id = client_id
        self.__client_secret = client_secret
        headers = headers or {}
        headers.update(
            {
                "X-IBM-client-id": client_id,
                "X-IBM-client-secret": client_secret,
            }
        )

        super(OpenBankingClientConfiguration, self).__init__(
            base_url=base_url, headers=headers, proxies=proxies
        )

    @property
    def client_id(self) -> str:
        """Return client_id."""
        return self.__client_id

    @property
    def client_secret(self) -> str:
        """Return client_secret."""
        return self.__client_secret


class OpenBankingHttpClient(RestApiHttpClient):
    """Sends requests to the Open Banking REST API endpoint."""

    def __init__(self, conf: OpenBankingClientConfiguration) -> None:
        """Constructs a :class:`OpenBankingHttpClient <OpenBankingHttpClient>`."""
        super().__init__()
        self.conf = conf

    @property
    def config(self) -> HttpClientConfiguration:
        """Return current configuration of Http Client."""
        return self.conf

    def get(self, url_suffix: str, **kwargs: Any) -> AnalyticsApiResponse:
        """Sends a GET request.

        Args:
            url_suffix: url path where requested will be sent.
            **kwargs: parameters for HTTP Request.

        Returns:
            requests.Response instance.
        """
        return self.request("GET", url_suffix, **kwargs)

    def post(self, url_suffix: str, json: Any, **kwargs: Any) -> AnalyticsApiResponse:
        """Sends a POST request.

        Args:
            url_suffix: url path where requested will be sent.
            json: data in format of dict or json.
            **kwargs: parameters for HTTP Request.

        Returns:
            requests.Response instance.
        """
        return self.request(method="POST", path=url_suffix, body=json, **kwargs)

    def _handle_error(self, api_response: AnalyticsApiResponse) -> None:
        if api_response.status_code == 401:
            raise OpenBankingUnauthorizedRequestError(
                api_response.request_id,
            )

        if api_response.status_code == 403:
            raise OpenBankingForbiddenRequestError(api_response.request_id)

        super()._handle_error(api_response)
