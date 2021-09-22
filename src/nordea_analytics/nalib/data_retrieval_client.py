from abc import ABC
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth
import yaml

from nordea_analytics.nalib.credentials import Login

config_path = str(Path(__file__).parent / "config.yml")
with open(config_path) as file:
    config = yaml.safe_load(file)


SERVICE_URL = config["service_url"]


class DataRetrievalServiceClient(Login, ABC):
    """Logs in to the Nordea Analytics REST API and sends requests to the service."""

    def __init__(
        self, username: str = None, password: str = None, login: bool = True
    ) -> None:
        """Initialization of class.

        Args:
            username: Windows g-login. Search for if None
            password: Windows password. Asked for if None
            login: boolean if should login with credentials or not. for testing.
        """
        self._service_name = "NordeaAnalytics"
        self.service_url = SERVICE_URL
        if login:
            Login.__init__(self, self._service_name, username, password)
            self._auth = HTTPBasicAuth(self.username + "@ONEADR", self.password)
            check = self._check_credentials()
            if not check.ok:
                if "Invalid UserName or Password" in check.reason:
                    Warning("Invalid UserName or Password. Try again")
                    Login.__init__(
                        self,
                        self._service_name,
                        username,
                        password,
                        new_credentials=True,
                    )
                    self._auth = HTTPBasicAuth(self.username + "@ONEADR", self.password)
                else:
                    Warning(check.reason)

    def get_response(self, request: dict, url_suffix: str) -> dict:
        """Gets the response from the data retrieval service for a given request.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
             Response in the form of json
        """
        response = requests.get(
            urljoin(self.service_url, url_suffix), params=request, auth=self._auth
        )
        response.raise_for_status()
        return response.json()

    def _check_credentials(self) -> Any:
        response = requests.get(
            urljoin(self.service_url, "indices/"),
            auth=self._auth,
        )
        return response
