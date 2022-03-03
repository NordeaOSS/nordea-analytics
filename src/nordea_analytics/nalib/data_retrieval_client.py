from abc import ABC
import json
import time
from typing import Any, Callable, Dict
from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth

from nordea_analytics.nalib.credentials import Login
from nordea_analytics.nalib.proxy_finder import ProxyFinder
from nordea_analytics.nalib.streaming_service import (
    ExternalStreamListener,
    InternalStreamListener,
    StreamListener,
)
from nordea_analytics.nalib.util import check_string, get_config

config = get_config()


SERVICE_URL = config["service_url"]
GET_PROXY_INFO = config["get_proxy_information"]


class DataRetrievalServiceClient(Login, ABC):
    """Logs in to the Nordea Analytics REST API and sends requests to the service."""

    def __init__(
        self,
        username: str = None,
        password: str = None,
        login: bool = None,
        service_url: str = None,
    ) -> None:
        """Initialization of class.

        Args:
            username: Windows g-login or client id(external users).
                Search for if None
            password: Windows password or client secret(external users).
                Asked for if None
            login: boolean if should login with credentials or not. for testing.
            service_url: string to which service should be pointed to. Used for festing
        """
        self._service_name = config["service_name"]
        self.service_url = service_url if service_url is not None else SERVICE_URL
        self._auth = None
        self.proxies = None
        self.live_data = None

        if login is not None:
            login = login
        else:
            login = config["login"]

        if GET_PROXY_INFO:
            self.proxy_finder = ProxyFinder(self.service_url)
            self.proxies = self.proxy_finder.proxies

        if login:
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

        Raises:
            ValueError: If error in request or takes to long to load.
        """
        response = self._get_response(request, url_suffix)
        if not response.ok:
            if "error_description" in response.text:
                raise ValueError(json.loads(response.text)["error_description"])
            elif response.status_code == 503:
                time.sleep(1)
                response = self._get_response(request, url_suffix)
            else:
                response.raise_for_status()

        if "job_url" in response.text:
            response_info = json.loads(response.text)
            t_end = time.time() + 60 * 8
            while not check_string(response.text, '"state":"completed"'):
                response = self._get_response(
                    {}, urljoin(self.service_url, "job/" + response_info["id"])
                )
                time.sleep(0.5)
                if time.time() > t_end:
                    raise ValueError("Took too long to retrieve values")

        return response.json()

    def get_post_get_response(self, request: dict, url_suffix: str) -> dict:
        """Posts a requests and gets response.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
            Response in the form of json.

        Raises:
            ValueError: If error in request or request has been removed.
        """
        post_response = self._post_response(
            request, urljoin(self.service_url, url_suffix)
        )
        if not post_response.ok:
            if "error_description" in post_response.text:
                raise ValueError(json.loads(post_response.text)["error_description"])
            elif post_response.status_code == 503:
                time.sleep(1)
                post_response = self._get_response(request, url_suffix)
            else:
                post_response.raise_for_status()

        get_response = self._get_response({}, "job/" + post_response.json()["id"])
        t_end = time.time() + 60 * 8
        while not check_string(get_response.text, '"state":"completed"'):
            get_response = self._get_response({}, "job/" + post_response.json()["id"])
            time.sleep(0.5)
            if time.time() > t_end:
                raise ValueError("Took too long to retrieve values")

        if get_response.json()["data"]["info"]["state"] == "completed":
            response = get_response.json()["data"]["response"]
            if "error" in response.keys() and response["error"] != {}:
                raise ValueError(response["error"])

        elif get_response.json()["data"]["info"]["state"] == "failed":  # some errors
            raise ValueError("Calculation failed")
        elif (
            get_response.json()["data"]["info"]["state"] == "removed"
        ):  # removed by server
            raise ValueError("Calculation has been removed")
        else:
            raise ValueError(
                "Calculation not completed. Stopped in state:"
                + get_response.json()["data"]["info"]["state"]
            )

        return get_response.json()["data"]["response"]

    def get_live_streamer(
        self, request: dict, url_suffix: str, update_method: Callable
    ) -> StreamListener:
        """Method for LiveDataRetrievalServiceClient."""
        pass

    def _get_response(self, request: dict, url_suffix: str) -> Any:
        """Gets the response from the service for a given request.

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

    def _post_response(self, request: dict, url_suffix: str) -> Any:
        """Gets the post response from the service given a request.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
             Post Response object with job information.
        """
        if config["use_headers"]:
            post_response = requests.post(
                urljoin(self.service_url, url_suffix),
                headers={
                    "X-IBM-client-id": self.username,
                    "X-IBM-client-secret": self.password,
                },
                params=request,
                proxies=self.proxies,
            )

        else:
            post_response = requests.post(
                urljoin(self.service_url, url_suffix),
                data=request,
                auth=self._auth,
                proxies=self.proxies,
            )
        return post_response

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


class LiveDataRetrievalServiceClient(DataRetrievalServiceClient):
    """Logs in and sends requests to the live streaming service."""

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
        super(LiveDataRetrievalServiceClient, self).__init__(username, password, login)
        self.streamListener = None
        self.live_data = None

    def get_live_streamer(
        self, request: Dict, url_suffix: str, update_method: Callable
    ) -> StreamListener:
        """Sends request and returns the stream listener which controls the live stream.

        Args:
            request: Dictionary of ISINs which should be streamed.
            url_suffix: Url suffix for a given method.
            update_method: reference to callable method where the streamed data is
                transformed into a presentable format.

        Returns:
            StreamListener for streaming.

        """
        isins = [request[x] for x in request]
        if "stream" in config["url_suffix"]["live_bond_key_figures"]:
            return InternalStreamListener(
                urljoin(self.service_url, url_suffix), isins, update_method, self._auth
            )
        else:
            return ExternalStreamListener(
                urljoin(self.service_url, url_suffix),
                isins,
                update_method,
                self._get_response,
            )
