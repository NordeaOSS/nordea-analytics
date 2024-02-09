import time
from abc import ABC, abstractmethod
from typing import Optional, Any, Dict, List, Union
from urllib.parse import urljoin

import requests

from nordea_analytics.nalib.exceptions import ApiServerError
from nordea_analytics.nalib.exceptions import HttpClientImproperlyConfigured
from nordea_analytics.nalib.http.errors import (
    BadRequestError,
    ForbiddenRequestError,
    UnauthorizedRequestError,
)
from nordea_analytics.nalib.http.errors import NotFoundRequestError, UnknownClientError
from nordea_analytics.nalib.http.models import AnalyticsApiResponse


class HttpClientConfiguration:
    """Contain parameters for HTTP requests."""

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        proxies: Optional[Dict[str, str]] = None,
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
        self.__base_url = base_url
        self.__headers = headers or {}
        self.__proxies = proxies or {}
        self.__max_retries = max_retries

    @property
    def base_url(self) -> str:
        """Base url for Analytics API service."""
        return self.__base_url

    @property
    def max_retries(self) -> int:
        """Maximum number of retries before exception will be thrown."""
        return self.__max_retries

    @property
    def headers(self) -> Dict[str, str]:
        """Common headers which will be sent with every request."""
        return self.__headers

    @property
    def proxies(self) -> Dict[str, str]:
        """If defined, proxies will be used with every request."""
        return self.__proxies


class RestApiHttpClient(ABC):
    """Sends requests to the Nordea Analytics REST API."""

    def __init__(self) -> None:
        """Create new instance of RestApiHttpClient."""
        self.__history: List[AnalyticsApiResponse] = []
        self.__session = None

    @property
    def history(self) -> List[AnalyticsApiResponse]:
        """A list of :class:`Response <Response>` objects with the history of the Responses.

        Any responses will end up here. The list is sorted from the oldest to the most recent response.

        Returns:
            A list of :class:`Response <Response>` objects.
        """
        return self.__history

    @property
    @abstractmethod
    def config(self) -> HttpClientConfiguration:
        """Return current configuration of Http Client."""
        pass

    @abstractmethod
    def get(self, url_suffix: str, **kwargs: Any) -> AnalyticsApiResponse:
        """Sends a GET request.

        Args:
            url_suffix: url path where requested will be sent.
            **kwargs: parameters for HTTP Request.

        Returns:
            A :class:`Response <Response>` object.
        """
        pass

    @abstractmethod
    def post(self, url_suffix: str, json: Any, **kwargs: Any) -> AnalyticsApiResponse:
        """Sends a POST request.

        Args:
            url_suffix: url path where requested will be sent.
            json: data in format of dict or json.
            **kwargs: parameters for HTTP Request.

        Returns:
            A :class:`Response <Response>` object.
        """
        pass

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Any] = None,
        body: Union[Any, None] = None,
        headers: Optional[Dict[str, str]] = None,
        proxies: Optional[Dict[str, str]] = None,
        **kwargs: Optional[Any],
    ) -> AnalyticsApiResponse:
        """Send a request to Analytics API server."""
        max_retries = self.config.max_retries
        headers = self.__prepare_headers(headers)
        proxies = proxies or self.config.proxies
        while max_retries > 0:
            max_retries = max_retries - 1
            raw_response = self.__execute(
                method=method,
                path=path,
                params=params,
                headers=headers,
                proxies=proxies,
                body=body,
                **kwargs,
            )
            api_response = AnalyticsApiResponse(raw_response)
            self.__history.append(api_response)

            if api_response.status_code == 200:
                return api_response

            # 503 Service Temporarily Unavailable == Too Many Requests
            if max_retries > 0 and api_response.status_code == 503:
                time.sleep(0.2)
                continue

            # Handler typical errors
            self._handle_error(api_response)

            # Raise in case typical error can't be handled
            raise ApiServerError("", api_response.raw_response.text)

        raise ApiServerError("Empty", "Can't get response")

    def _get_session(self) -> requests.Session:
        """Create new session."""
        if self.__session is None:
            self.__session = requests.Session()

        return self.__session

    def _handle_error(self, api_response: AnalyticsApiResponse) -> None:
        """Handle response error codes."""
        http_code = api_response.status_code
        request_id = api_response.request_id
        error_description = api_response.error_description or "Unknown error"
        if http_code == 400:
            raise BadRequestError(request_id, error_description)

        if http_code == 401:
            raise UnauthorizedRequestError(request_id, error_description)

        if http_code == 403:
            raise ForbiddenRequestError(request_id, error_description)

        if http_code == 404:
            raise NotFoundRequestError(
                request_id,
                f"[{api_response.method}] {api_response.url}",
            )

        if 404 < http_code < 500:
            raise UnknownClientError(request_id, error_description)

        raise ApiServerError(api_response.request_id, error_description)

    def __prepare_headers(self, headers: Union[Dict[str, str], None]) -> Dict[str, str]:
        request_headers = {}
        if self.config.headers is not None:
            request_headers.update(self.config.headers)
        if headers is not None:
            request_headers.update(headers)
        return request_headers

    def __execute(
        self,
        method: str,
        path: str,
        params: Any,
        headers: Dict[str, str],
        proxies: Dict[str, str],
        body: Union[Any, None],
        **kwargs: Any,
    ) -> requests.Response:
        full_url = urljoin(self.config.base_url, path)
        session = self._get_session()
        raw_response = session.request(
            method,
            full_url,
            params=params,
            headers=headers,
            json=body,
            proxies=proxies,
            **kwargs,
        )
        return raw_response
