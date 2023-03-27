from typing import Any, Dict, Union
from urllib.parse import urljoin

import requests

from nordea_analytics.nalib.exceptions import HttpClientImproperlyConfigured
from nordea_analytics.nalib.http.core import HttpClientConfiguration, RestApiHttpClient


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
        if not client_id:
            raise HttpClientImproperlyConfigured("client_id is not set.")
        if not client_secret:
            raise HttpClientImproperlyConfigured("client_secret is not set.")

        if headers is None:
            headers = {}

        headers["X-IBM-client-id"] = client_id
        headers["X-IBM-client-secret"] = client_secret

        super(OpenBankingClientConfiguration, self).__init__(
            base_url=base_url, headers=headers, proxies=proxies
        )


class OpenBankingHttpClient(RestApiHttpClient):
    """Sends requests to the Open Banking REST API endpoint."""

    def __init__(self, conf: OpenBankingClientConfiguration) -> None:
        """Constructs a :class:`OpenBankingHttpClient <OpenBankingHttpClient>`."""
        super(OpenBankingHttpClient, self).__init__()
        self.conf = conf

    @property
    def config(self) -> HttpClientConfiguration:
        """Return current configuration of Http Client."""
        return self.conf

    def get(self, url_suffix: str, **kwargs: Any) -> requests.Response:
        """Sends a GET request.

        Args:
            url_suffix: url path where requested will be sent.
            **kwargs: parameters for HTTP Request.

        Returns:
            requests.Response instance.
        """
        max_retries = self.conf.max_retries
        return super(OpenBankingHttpClient, self)._proceed_response(
            max_retries, lambda: self._request("get", url_suffix, **kwargs)
        )

    def post(self, url_suffix: str, json: Any, **kwargs: Any) -> requests.Response:
        """Sends a POST request.

        Args:
            url_suffix: url path where requested will be sent.
            json: data in format of dict or json.
            **kwargs: parameters for HTTP Request.

        Returns:
            requests.Response instance.
        """
        max_retries = self.conf.max_retries
        kwargs["json"] = json
        return super(OpenBankingHttpClient, self)._proceed_response(
            max_retries, lambda: self._request("post", url_suffix, **kwargs)
        )

    def _request(
        self, method: str, url_suffix: str, **kwargs: Any
    ) -> requests.Response:
        url = urljoin(self.conf.base_url, url_suffix)
        self.prepare_request_params(kwargs)
        return requests.request(method, url, **kwargs)
