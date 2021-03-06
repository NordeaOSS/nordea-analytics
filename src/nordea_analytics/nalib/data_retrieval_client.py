import time
from typing import Dict
import warnings

import requests
from requests import Response

from nordea_analytics.nalib import exceptions
from nordea_analytics.nalib.http.core import RestApiHttpClient
from nordea_analytics.nalib.live_keyfigures.core import HttpStreamIterator
from nordea_analytics.nalib.util import check_string


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

    def get_response(self, request: dict, url_suffix: str) -> dict:
        """Gets the response from _get_response function for a given request.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
            Response in the form of json.
        """
        response = self._get_response(request, url_suffix)
        response_json = response.json()
        if "info" in response_json and "job_url" in response_json["info"]:
            background_response = self._proceed_background_job(response_json)
            _response = self._check_errors(background_response)
        else:
            _response = response_json["data"]
            if "failed_queries" in _response.keys():
                if not _response["failed_queries"] == []:
                    warnings.warn(str(_response["failed_queries"]))

        return _response

    def get_post_get_response(self, request: dict, url_suffix: str) -> dict:
        """Posts a requests for background calculation and gets response.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
            Response in the form of json.
        """
        post_response = self._post_response(request, url_suffix)
        background_response = self._proceed_background_job(post_response.json())
        return self._check_errors(background_response)

    def get_live_streamer(self) -> HttpStreamIterator:
        """Method return HttpStreamIterator which allow iteration over stream."""
        return self.stream_listener

    def _proceed_background_job(self, response_info: Dict) -> requests.Response:
        """Proceed background response and retrieve job data from server."""
        t_end = time.time() + 60 * 8
        while time.time() < t_end:
            response = self._get_response(
                {},
                "job/" + response_info["id"],
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

    def _get_response(self, request: dict, url_suffix: str) -> requests.Response:
        return self.http_client.get(url_suffix, params=request)

    def _post_response(self, request: dict, url_suffix: str) -> requests.Response:
        return self.http_client.post(url_suffix, request)
