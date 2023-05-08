from abc import ABC, abstractmethod
import time
from typing import Any, Callable, Dict, List

import requests

from nordea_analytics.nalib.exceptions import ApiServerError
from nordea_analytics.nalib.exceptions import HttpClientImproperlyConfigured


class HttpClientConfiguration:
    """Contain parameters for HTTP requests."""

    def __init__(
        self,
        base_url: str,
        headers: Dict[str, str] = None,
        proxies: Dict[str, str] = None,
        max_retries: int = 10,
    ) -> None:
        """Constructs a :class:`HttpClientConfiguration <HttpClientConfiguration>`.

        Args:
            base_url: base URL for all http requests.
            headers: (optional) Dictionary of HTTP Headers to send with the request.
            proxies: (optional) Dictionary mapping protocol or protocol and hostname to the URL of the proxy.
            max_retries: (optional) Maximum number of retries for HTTP requests.

        Raises:
            HttpClientImproperlyConfigured: If `base_url` is not set.
        """
        if not base_url:
            raise HttpClientImproperlyConfigured("base_url is not set.")

        self.service_name = "Nordea Analytics API"
        self.base_url = base_url
        self.headers = headers or {}
        self.proxies = proxies or {}
        self.max_retries = max_retries


class RestApiHttpClient(ABC):
    """Sends requests to the Nordea Analytics REST API."""

    def __init__(self) -> None:
        """Create new instance of RestApiHttpClient."""
        self._history: List[requests.Response] = []

    @property
    def history(self) -> List[requests.Response]:
        """A list of :class:`Response <Response>` objects with the history of the Responses.

        Any responses will end up here. The list is sorted from the oldest to the most recent response.

        Returns:
            A list of :class:`Response <Response>` objects.
        """
        return self._history

    @property
    @abstractmethod
    def config(self) -> HttpClientConfiguration:
        """Return current configuration of Http Client."""
        pass

    @abstractmethod
    def get(self, url_suffix: str, **kwargs: Any) -> requests.Response:
        """Sends a GET request.

        Args:
            url_suffix: url path where requested will be sent.
            **kwargs: parameters for HTTP Request.

        Returns:
            A :class:`Response <Response>` object.
        """
        pass

    @abstractmethod
    def post(self, url_suffix: str, json: Any, **kwargs: Any) -> requests.Response:
        """Sends a POST request.

        Args:
            url_suffix: url path where requested will be sent.
            json: data in format of dict or json.
            **kwargs: parameters for HTTP Request.

        Returns:
            A :class:`Response <Response>` object.
        """
        pass

    def prepare_request_params(self, params: Dict) -> None:
        """Create parameters for HTTP Request.

        Args:
            params: parameters for HTTP Request.
        """
        headers = params.setdefault("headers", {})
        headers.update(self.config.headers)

        proxies = params.setdefault("proxies", {})
        proxies.update(self.config.proxies)

    def _proceed_response(
        self, max_retries: int, send_callable: Callable[[], requests.Response]
    ) -> requests.Response:
        """Proceed the response in accordance with server logic."""
        self.history.clear()

        while max_retries != 0:
            max_retries = max_retries - 1
            response = send_callable()

            self.history.append(response)

            if response.ok:
                return response

            # 503 Service Temporarily Unavailable == Too Many Requests
            if max_retries > 0 and response.status_code == 503:
                time.sleep(0.2)
            elif "error_description" in response.text:
                error_json = response.json()
                raise ApiServerError(
                    error_id=response.headers.get("x-request-id"),
                    error_description=f'{error_json["error_description"]}',
                )
            else:
                break

        return response
