import time
from typing import Any, Dict

import requests
from requests import Response

from nordea_analytics.nalib import exceptions
from nordea_analytics.nalib.exceptions import AnalyticsWarning, CustomWarning
from nordea_analytics.nalib.http.core import RestApiHttpClient
from nordea_analytics.nalib.live_keyfigures.core import HttpStreamIterator
from nordea_analytics.nalib.util import AnalyticsResponseError, RequestMethod


class DataRetrievalServiceClient(object):
    """Logs in to the Nordea Analytics REST API and sends requests to the service."""

    def __init__(
        self, http_client: RestApiHttpClient, stream_listener: HttpStreamIterator
    ) -> None:
        """Constructs a :class:`DataRetrievalServiceClient <DataRetrievalServiceClient>`.

        Args:
            http_client: instance of RestApiHttpClient which will perform HTTP requests.
            stream_listener: instance of HttpStreamIterator which will iterate over HTTP stream.
        """
        self.http_client = http_client
        self.stream_listener = stream_listener
        self._last_request: Any = {}

    @property
    def diagnostic(self) -> Dict:
        """Return a diagnostic information."""
        diag = self._last_request
        if self.http_client.history:
            last_response = self.http_client.history.pop()
            diag["response"] = {
                "id": last_response.headers["x-request-id"],
                "status": last_response.status_code,
                "elapsed": last_response.elapsed.total_seconds() * 1000,
            }
        return diag

    def get_response(
        self, request: dict, url_suffix: str, request_method: RequestMethod
    ) -> dict:
        """Gets the response from _get_response function for a given request.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method
            request_method: Enum request method

        Returns:
            Response in the form of json.

        Raises:
            Exception: If exception is raised from API.
        """
        response: Response
        if request_method == RequestMethod.Get:
            response = self._get_response(url_suffix, params=request)
        elif request_method == RequestMethod.Post:
            response = self._post_response(request, url_suffix)

        self._verify_response(response.json())

        response_json = response.json()
        if "info" in response_json and "job_url" in response_json["info"]:
            log_token = response.headers.get("X-Request-ID", None)
            params = (
                {"headers": {"X-Request-ID-Override": log_token}}
                if log_token is not None
                else {}
            )
            background_response = self._proceed_background_job(response_json, **params)
            _response = self._check_errors(background_response)
        else:
            if "data" in response_json:
                _response = response_json["data"]
                if "failed_queries" in _response.keys():
                    if not _response["failed_queries"] == []:
                        CustomWarning(
                            str(_response["failed_queries"]), AnalyticsWarning
                        )
            elif (
                "response_status" in response_json
                and "error_code" in response_json["response_status"]
            ):
                raise Exception(
                    AnalyticsResponseError(response_json["response_status"]["message"])
                )

        return _response

    def get_post_get_response(self, request: dict, url_suffix: str) -> dict:
        """Posts a requests for background calculation and gets response.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
            Response in the form of json.
        """
        response = self.get_response(request, url_suffix, RequestMethod.Post)
        return response

    def get_response_asynchronous(self, request: dict, url_suffix: str) -> dict:
        """Posts a requests for background calculation and gets response.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
            Response in the form of json.
        """
        post_response = self._post_response(request, url_suffix)
        self._verify_response(post_response.json())

        log_token = post_response.headers.get("X-Request-ID", None)
        params = (
            {"headers": {"X-Request-ID-Override": log_token}}
            if log_token is not None
            else {}
        )
        background_response = self._proceed_background_job(
            post_response.json(), **params
        )
        return self._check_errors(background_response)

    def get_live_streamer(self) -> HttpStreamIterator:
        """Method return HttpStreamIterator which allow iteration over stream."""
        return self.stream_listener

    def _proceed_background_job(
        self, response_info: Dict, **kwargs: Any
    ) -> requests.Response:
        """Proceed background response and retrieve job data from server."""
        t_end = time.time() + 60 * 8
        while time.time() < t_end:
            response = self._get_response(
                "job/" + response_info["id"],
                **kwargs,
            )

            if '"state":"completed"' in response.text:
                return response
            elif '"state":"failed"' in response.text:
                raise exceptions.BackgroundCalculationFailed(
                    error_id=response.headers["x-request-id"],
                    error_description="Background job failed to proceed",
                )
            elif (
                '"state":"new"' in response.text
                or '"state":"processing"' in response.text
                or '"state":"rescheduled"' in response.text
            ):
                time.sleep(0.2)
            else:
                raise exceptions.BackgroundCalculationFailed(
                    error_id=response.headers["x-request-id"],
                    error_description=f"Unknown status: {response.text}",
                )

        raise exceptions.BackgroundCalculationTimeout()

    def _refresh_diagnostic_info(self, **kwargs: Any) -> None:
        self._last_request.clear()
        self._last_request = kwargs

    @staticmethod
    def _verify_response(response: Dict) -> None:
        if "httpCode" in response and response["httpCode"] != 200:
            raise exceptions.ApiServerUnauthorized(response["moreInformation"])

    @staticmethod
    def _check_errors(get_response: Response) -> Dict:
        _response = get_response.json()["data"]["response"]
        if (
            "error" in get_response.text
            and "failed_calculation" not in get_response.text
        ):
            if "error" in _response.keys() and _response["error"] == {}:
                del _response["error"]
            elif (
                "data" in _response
                and "error" in _response["data"]
                and _response["data"]["error"] == {}
            ):
                _response = _response["data"]
                del _response["error"]
            else:
                raise AnalyticsResponseError(
                    _response["data"]["failed_calculation"]["info"]
                )

        if "failed_calculation" in get_response.text:
            if (
                "failed_calculation" in _response.keys()
                and _response["failed_calculation"] == {}
            ):
                del _response["failed_calculation"]
            elif (
                "data" in _response.keys()
                and "info" in _response["data"]["failed_calculation"]
                and _response["data"]["failed_calculation"]["info"] == ""
            ):
                _response = _response["data"]
                del _response["failed_calculation"]
            elif (
                "failed_calculation" in _response
                and "info" in _response["failed_calculation"]
                and _response["failed_calculation"]["info"] == ""
            ):
                del _response["failed_calculation"]
            elif (
                "failed_calculation" in _response
                and "error" in _response["failed_calculation"]
            ):
                raise AnalyticsResponseError(_response["failed_calculation"]["error"])
            elif "data" in _response:
                raise AnalyticsResponseError(
                    _response["data"]["failed_calculation"]["info"]
                )
            else:
                CustomWarning(
                    str(_response["failed_calculation"]["info"]), AnalyticsWarning
                )

        if "data" in _response.keys():
            _response = _response["data"]

        return _response

    def _get_response(self, url_suffix: str, **kwargs: Any) -> requests.Response:
        self._refresh_diagnostic_info(method="GET", url_suffix=url_suffix, **kwargs)
        return self.http_client.get(url_suffix, **kwargs)

    def _post_response(
        self, request: dict, url_suffix: str, **kwargs: Any
    ) -> requests.Response:
        self._refresh_diagnostic_info(
            method="POST", request=request, url_suffix=url_suffix, **kwargs
        )
        return self.http_client.post(url_suffix, request, **kwargs)
