from abc import ABC
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth
import yaml

from nordea_analytics.nalib.credentials import Login
from nordea_analytics.nalib.proxy_finder import ProxyFinder

config_path = str(Path(__file__).parent / "config.yml")
with open(config_path) as file:
    config = yaml.safe_load(file)


SERVICE_URL = config["service_url"]
GET_PROXY_INFO = config["get_proxy_information"]


class DataRetrievalServiceClient(Login, ABC):
    """Logs in to the Nordea Analytics REST API and sends requests to the service."""

    def __init__(
        self, username: str = None, password: str = None, login: bool = None
    ) -> None:
        """Initialization of class.

        Args:
            username: Windows g-login or client id(external users).
                Search for if None
            password: Windows password or client secret(external users).
                Asked for if None
            login: boolean if should login with credentials or not. for testing.
        """
        self._service_name = config["service_name"]
        self.service_url = SERVICE_URL
        self._auth = None
        self.proxies = None
        if GET_PROXY_INFO:
            self.proxy_finder = ProxyFinder(self.service_url)
            self.proxies = self.proxy_finder.proxies

        if login is None and config["login"]:
            Login.__init__(self, self._service_name, username, password)
            self._auth = HTTPBasicAuth(
                self.username + config["user_suffix"], self.password
            )

        # check of proxies and login info if needed
        if self.proxies is not None:
            self._check_proxies()
        if self._auth is not None:
            self._check_credentials()

    def get_response(self, request: dict, url_suffix: str) -> dict:
        """Gets the response from _get_response function for a given request.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
            Response in the form of json.
        """
        response = self._get_response(request, url_suffix)
        response.raise_for_status()
        return response.json()

    def _get_response(self, request: dict, url_suffix: str) -> Any:
        """Gets the response from the data retrieval service for a given request.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
             Response object.
        """
        if config["use_headers"]:
            response = requests.get(
                urljoin(self.service_url, url_suffix),
                headers={
                    "X-IBM-client-id": self.username,
                    "X-IBM-client-secret": self.password,
                },
                params=request,
                proxies=self.proxies,
            )
        else:
            response = requests.get(
                urljoin(self.service_url, url_suffix),
                params=request,
                auth=self._auth,
                proxies=self.proxies,
            )
        return response

    def _check_credentials(self) -> Any:
        response = self._get_response(
            {"": ""}, config["url_suffix"]["index_composition"]
        )
        if not response.ok:
            if "Unauthorized" in response.text:
                Warning("Invalid UserName or Password. Try again")
                Login.__init__(
                    self,
                    self._service_name,
                    self.username,
                    self.password,
                    new_credentials=True,
                )
                self._auth = HTTPBasicAuth(
                    self.username + config["user_suffix"], self.password
                )
                check = self._get_response(
                    {"": ""}, config["url_suffix"]["index_composition"]
                )
                if not check.ok:
                    Warning(
                        "Invalid UserName or Password. "
                        "Please try running the code again."
                    )
            else:
                Warning(response.reason)

    def _check_proxies(self) -> Any:
        try:
            self._get_response({"": ""}, config["url_suffix"]["index_composition"])
        except Exception as e:
            if type(e) == requests.exceptions.ConnectionError:
                # if proxy error, try deleting the .proxy_info file
                # and look for proxy information again

                proxy_path = self.proxy_finder.proxy_path
                if proxy_path.exists():
                    proxy_path.unlink()
                new_proxy_finder = ProxyFinder(self.service_url)
                proxy_info = new_proxy_finder.proxies
                # try once again
                try:
                    self._get_response(
                        {"": ""}, config["url_suffix"]["index_composition"]
                    )
                    self.proxies = proxy_info
                except Exception as e:
                    raise e
