from abc import ABC
import json
import time
from typing import Any, Callable, Dict
from urllib.parse import urljoin
import warnings

import requests
from requests import Response
from requests.auth import HTTPBasicAuth

from nordea_analytics.nalib import auth
from nordea_analytics.nalib.credentials import Login
from nordea_analytics.nalib.proxy_finder import ProxyFinder
from nordea_analytics.nalib.streaming_service import (
    LiveKeyfigureListener,
    LiveKeyfigureStreamListener,
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
        streaming: bool = False,
    ) -> None:
        """Initialization of class.

        Args:
            username: Windows g-login or client id(external users).
                Search for if None
            password: Windows password or client secret(external users).
                Asked for if None
            login: boolean if should login with credentials or not. for testing.
            service_url: string to which service should be pointed to. Used for testing.
            streaming: boolean if should use streaming. Used for testing.
        """
        self._service_name = config["service_name"]
        self.service_url = service_url if service_url is not None else SERVICE_URL
        self.streaming = True
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

        self._session: Dict[str, Any] = {}

    def get_response(self, request: dict, url_suffix: str) -> dict:
        """Gets the response from _get_response function for a given request.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
            Response in the form of json.

        Raises:
            ValueError: If request takes to long to load.
        """
        response = self._get_response(request, url_suffix)

        if "job_url" in response.text:
            response_info = json.loads(response.text)
            t_end = time.time() + 60 * 8
            while True:
                if time.time() > t_end:
                    raise ValueError("Took too long to retrieve values")
                response = self._get_response(
                    {}, urljoin(self.service_url, "job/" + response_info["id"])
                )

                if check_string(response.text, '"state":"completed"'):
                    break
                else:
                    time.sleep(0.2)

            _response = self._check_errors(response)
        else:
            _response = response.json()["data"]
            if "failed_queries" in _response.keys():
                if not _response["failed_queries"] == []:
                    warnings.warn(str(_response["failed_queries"]))

        return _response

    def get_post_get_response(self, request: dict, url_suffix: str) -> dict:
        """Posts a requests and gets response.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
            Response in the form of json.

        Raises:
            ValueError: If error in request or takes to long to load.
        """
        post_response = self._post_response(request, url_suffix)

        t_end = time.time() + 60 * 8
        while True:
            if time.time() > t_end:
                raise ValueError("Took too long to retrieve values")

            get_response = self._get_response({}, "job/" + post_response.json()["id"])
            if check_string(get_response.text, '"state":"completed"'):
                break
            else:
                time.sleep(0.2)
        _response = self._check_errors(get_response)
        return _response

    def get_live_streamer(
        self, request: dict, url_suffix: str, update_method: Callable
    ) -> StreamListener:
        """Method for LiveDataRetrievalServiceClient."""
        pass

    @staticmethod
    def _check_errors(get_response: Response) -> Dict:

        _response = get_response.json()["data"]["response"]
        if check_string(get_response.text, "error"):
            if "error" in _response.keys() and _response["error"] == {}:
                del _response["error"]
            elif _response["data"]["error"] == {}:
                _response = _response["data"]
                del _response["error"]
            else:
                raise ValueError(_response["data"]["failed_calculation"]["info"])

        if check_string(get_response.text, "failed_calculation"):
            if (
                "failed_calculation" in _response.keys()
                and _response["failed_calculation"] == {}
            ):
                del _response["failed_calculation"]
            elif (
                "data" in _response.keys()
                and _response["data"]["failed_calculation"]["info"] == ""
            ):
                _response = _response["data"]
                del _response["failed_calculation"]
            elif (
                "failed_calculation" in _response
                and _response["failed_calculation"]["info"] == ""
            ):
                del _response["failed_calculation"]
            else:
                raise ValueError(_response["data"]["failed_calculation"]["info"])

        if "data" in _response.keys():
            _response = _response["data"]

        return _response

    def _get_response(
        self, request: dict, url_suffix: str, return_invalid_request: bool = False
    ) -> Any:
        """Gets the response from the service for a given request.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method
            return_invalid_request: When True, invalid requests are returned.
                Only used in _check_credentials to see if error message
                includes unauthorized string

        Returns:
             Response object.

        Raises:
            ValueError: If error in request.
            raise_for_status: If error in request.
        """
        self._check_auth()
        max_retries = 10
        while max_retries != 0:
            max_retries = max_retries - 1
            if config["use_headers"]:
                response = requests.get(
                    urljoin(self.service_url, url_suffix),
                    headers={
                        "X-IBM-client-id": self.username,
                        "X-IBM-client-secret": self.password,
                    },
                    params=request,
                    proxies=self.proxies,
                    **self._session,
                )
            else:
                response = requests.get(
                    urljoin(self.service_url, url_suffix),
                    params=request,
                    auth=self._auth,
                    proxies=self.proxies,
                    **self._session,
                )

            if response.status_code == 401:
                self._check_auth(True)
                max_retries = max_retries + 1
                continue

            if response.ok:  # status_code == 200
                return response
            if return_invalid_request and response.status_code == 400:
                return response
            if max_retries != 0 and response.status_code == 503:
                time.sleep(0.2)
            else:
                if "error_description" in response.text:
                    raise ValueError(
                        json.loads(response.text)["error_description"]
                        + "\n Error_code: "
                        + json.loads(response.text)["error_code"]
                    )

                raise response.raise_for_status()

    def _post_response(self, request: dict, url_suffix: str) -> Any:
        """Gets the post response from the service given a request.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
             Post Response object with job information.

        Raises:
            ValueError: If error in request.
            raise_for_status: If error in request.
        """
        self._check_auth()
        max_retries = 10
        while max_retries != 0:
            max_retries = max_retries - 1
            if config["use_headers"]:
                post_response = requests.post(
                    urljoin(self.service_url, url_suffix),
                    headers={
                        "X-IBM-client-id": self.username,
                        "X-IBM-client-secret": self.password,
                    },
                    json=request,
                    proxies=self.proxies,
                    **self._session,
                )
            else:
                post_response = requests.post(
                    urljoin(self.service_url, url_suffix),
                    json=request,
                    auth=self._auth,
                    proxies=self.proxies,
                    **self._session,
                )

            if post_response.status_code == 401:
                self._check_auth(True)
                max_retries = max_retries + 1
                continue

            if post_response.ok:  # status_code == 200
                return post_response
            if max_retries != 0 and post_response.status_code == 503:
                time.sleep(0.2)
            else:
                if "error_description" in post_response.text:
                    raise ValueError(
                        json.loads(post_response.text)["error_description"]
                        + "\n Error_code: "
                        + json.loads(post_response.text)["error_code"]
                    )

                raise post_response.raise_for_status()

    def _check_credentials(self) -> Any:
        response = self._get_response(
            {"": ""},
            config["url_suffix"]["index_composition"],
            return_invalid_request=True,
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

    def _check_auth(self, refresh: bool = False) -> None:
        if refresh and "cookies" in self._session:
            del self._session["cookies"]

        if "cookies" not in self._session:
            cookies = auth.authenticate()
            if not cookies:
                raise ValueError("Authentication not supported!")

            self._session["cookies"] = cookies


class LiveDataRetrievalServiceClient(DataRetrievalServiceClient):
    """Logs in and sends requests to the live streaming service."""

    def __init__(
        self,
        username: str = None,
        password: str = None,
        login: bool = None,
        service_url: str = None,
        streaming: bool = False,
    ) -> None:
        """Initialization of class.

        Args:
            username: Windows g-login or client id(external users).
                Search for if None
            password: Windows password or client secret(external users).
                Asked for if None
            login: boolean if should login with credentials or not. for testing.
            service_url: string to which service should be pointed to. Used for testing.
            streaming: boolean if should use streaming. Used for testing.
        """
        super(LiveDataRetrievalServiceClient, self).__init__(
            username, password, login, service_url, streaming
        )
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
        if "stream" in url_suffix:
            return LiveKeyfigureStreamListener(
                urljoin(self.service_url, url_suffix), isins, update_method, self._auth
            )
        else:
            return LiveKeyfigureListener(
                urljoin(self.service_url, url_suffix),
                isins,
                update_method,
                self._get_response,
            )
