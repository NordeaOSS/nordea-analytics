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
    """A client for making API requests to the Nordea Analytics REST API and handling responses.

    Attributes:
        http_client (RestApiHttpClient): The HTTP client used to make requests.
        stream_listener (HttpStreamIterator): Iterator for consuming Server Events streams.

    Method
       get_response(request: dict, url_suffix: str, request_method: RequestMethod = RequestMethod.Get) -> dict:
           Sends a request to the API and returns the response as a dictionary.

       get_post_get_response(request: dict, url_suffix: str, request_method: RequestMethod = RequestMethod.Get) -> dict:
           Sends a request to the API and returns the response as a dictionary.

       diagnostic() -> dict:
            Returns diagnostic information about the last API request made by the client.
    """

    def __init__(
        self, http_client: RestApiHttpClient, stream_listener: HttpStreamIterator
    ) -> None:
        """Constructs a :class:`DataRetrievalServiceClient <DataRetrievalServiceClient>`.

        Args:
            http_client: The HTTP client used to make requests.
            stream_listener: Iterator for consuming Server Events streams.
        """
        self.http_client = http_client
        self.stream_listener = stream_listener
        self._last_request: Any = {}

    @property
    def diagnostic(self) -> Dict:
        """Return diagnostic information about the last request."""
        diag = self._last_request.copy()

        if self.http_client.history:
            last_response = self.http_client.history[-1]
            diag["response"] = {
                "id": last_response.headers.get("x-request-id"),
                "status": last_response.status_code,
                "elapsed": last_response.elapsed.total_seconds() * 1000,
            }

        return diag

    def get_response(
        self,
        request: Dict,
        url_suffix: str,
        request_method: RequestMethod = RequestMethod.Get,
    ) -> Dict:
        """Sends a request to the API and returns the response.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method
            request_method: Request method type

        Returns:
            Response in the form of Dict.
        """
        response = self._get_or_post_response(request, url_suffix, request_method)
        response_json = response.json()

        return self._parse_response(response, response_json)

    def get_post_get_response(self, request: Dict, url_suffix: str) -> Dict:
        """Posts a requests for background calculation and gets response.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method

        Returns:
            Response in the form of json.
        """
        response = self.get_response(request, url_suffix, RequestMethod.Post)
        return response

    def get_response_asynchronous(self, request: Dict, url_suffix: str) -> Dict:
        """Sends a request for a background calculation and retrieves the response.

        Args:
            request (Dict): The request data in dictionary form.
            url_suffix (str): The URL suffix for the given method.

        Returns:
            The response data in JSON format.

        This function sends a POST request for a background calculation, verifies that the response is valid,
        proceeds with the background job, and checks for errors in the response.

        If an error is found, this function raises an 'AnalyticsResponseError' exception.
        """
        post_response = self._post_response(request, url_suffix)
        self._verify_response(post_response)

        log_token = post_response.headers.get("X-Request-ID")
        params = {"headers": {"X-Request-ID-Override": log_token}} if log_token else {}
        background_response = self._proceed_background_job(
            post_response.json(), **params
        )
        return self._check_errors(background_response)

    def get_live_streamer(self) -> HttpStreamIterator:
        """Method return HttpStreamIterator which allow iteration over stream."""
        return self.stream_listener

    @staticmethod
    def _verify_response(response: requests.Response) -> None:
        """Verifies that an HTTP response is valid and raises an exception if it is not.

        Args:
            response (requests.Response): The HTTP response object to verify.

        Raises:
            ApiServerUnauthorized: If the response status code is between 400 and 499 and the response
                content type is 'application/json', and the response contains a 'moreInformation' key in its JSON body.
            AnalyticsResponseError: If the response status code is 400 or higher.

        This function checks whether the response has a content type of 'application/json', and if so, extracts the
        JSON body of the response. If the response status code is 40x this function raises an 'ApiServerUnauthorized'
        exception with the value of the 'moreInformation' key as the error description.

        Otherwise, if the response status code is 500 or higher, this function raises an 'AnalyticsResponseError'
        with the response status code and reason as the error description.
        """

        if "application/json" in response.headers.get("content-type", ""):
            json_response = response.json()
            if 400 <= response.status_code < 500:
                more_information = json_response.get("moreInformation", "Unknown error")
                raise exceptions.ApiServerUnauthorized(more_information)

        if response.status_code > 500:
            error_description = f"Status code: {response.status_code} {response.reason}"
            raise exceptions.AnalyticsResponseError(error_description)

    @staticmethod
    def _check_errors(get_response: Response) -> Dict:
        """Extracts and cleans up the response data from a successful Analytics API call.

        Args:
            get_response: The response object from the Analytics API call.

        Returns:
            A dictionary containing the data from the Analytics API call, with any unnecessary
            or redundant information removed.
        """

        response = get_response.json()["data"]["response"]

        if "failed_calculation" in response:
            failed_calculation = response["failed_calculation"]
            if failed_calculation:
                for error_message in failed_calculation:
                    CustomWarning(error_message, AnalyticsWarning)
            else:
                del response["failed_calculation"]

        if "data" in response:
            return response["data"]

        return response

    @staticmethod
    def _check_failed_queries(response_json: Dict) -> Dict:
        """Checks for errors in the data response.

        Args:
            response_json: Response in the form of json.

        Returns:
            Parsed response in the form of dict.
        """

        response_data = response_json["data"]
        if "failed_queries" in response_data and response_data["failed_queries"]:
            CustomWarning(str(response_data["failed_queries"]), AnalyticsWarning)

        return response_data

    def _get_or_post_response(
        self, request: Dict, url_suffix: str, request_method: RequestMethod
    ) -> Response:
        """Gets or posts the response based on the request method.

        Args:
            request: Request in the form of dictionary
            url_suffix: Url suffix for a given method
            request_method: Request method type

        Returns:
            Response object.

        Raises:
            ValueError: If invalid method requested.
        """

        if request_method == RequestMethod.Get:
            return self._get_response(url_suffix, params=request)

        if request_method == RequestMethod.Post:
            return self._post_response(request, url_suffix)

        raise ValueError(f"Invalid request method: {request_method}")

    def _parse_response(self, response: Response, response_json: Dict) -> Dict:
        """Parses the API response based on its format.

        Args:
            response (Response): A Response object from the API call.
            response_json (Dict): The API response in JSON format.

        Returns:
            Dict: The parsed response in the form of a dictionary.

        Raises:
            AnalyticsResponseError: If the response contains an error code and a message.
            ValueError: If the response has an unknown format.
        """

        if "info" in response_json and "job_url" in response_json["info"]:
            log_token = response.headers.get("X-Request-ID", None)
            headers = (
                {"headers": {"X-Request-ID-Override": log_token}} if log_token else {}
            )
            background_response = self._proceed_background_job(response_json, **headers)
            return self._check_errors(background_response)

        if "data" in response_json:
            return self._check_failed_queries(response_json)

        if (
            "response_status" in response_json
            and "error_code" in response_json["response_status"]
        ):
            raise AnalyticsResponseError(response_json["response_status"]["message"])

        raise ValueError("Unknown response format")

    def _proceed_background_job(
        self, response_info: Dict[str, Any], **kwargs: Any
    ) -> requests.Response:
        """This method continuously polls the status of a background job until it completes, fails, or times out.

        Args:
            response_info (Dict[str, Any]): A dictionary containing the background job's ID.
            **kwargs (Any): Additional keyword arguments to be passed to the `_get_response` function.

        Returns:
            requests.Response: The response object from the completed job.

        Raises:
            BackgroundCalculationFailed: If the job fails or an unknown status is encountered.
            BackgroundCalculationTimeout: If the job does not complete within the specified timeout.

        This function continuously checks the status of a background job using its ID found in `response_info`.
        It returns the response when the job is completed, raises a `BackgroundCalculationFailed` exception if
        the job fails, or raises a `BackgroundCalculationTimeout` exception if the job does not finish within
        the specified timeout.
        """

        timeout_seconds = 60 * 8
        end_time = time.monotonic() + timeout_seconds

        while time.monotonic() < end_time:
            response = self._get_response(f"job/{response_info['id']}", **kwargs)
            state = response.json().get("data", {}).get("info", {}).get("state", None)

            if state == "completed":
                return response

            if state == "failed":
                raise exceptions.BackgroundCalculationFailed(
                    error_id=response.headers["x-request-id"],
                    error_description="Background job failed to proceed",
                )

            if state in ("new", "processing", "rescheduled"):
                time.sleep(0.2)
                continue

            raise exceptions.BackgroundCalculationFailed(
                error_id=response.headers["x-request-id"],
                error_description=f"Unknown status: {response.text}",
            )

        raise exceptions.BackgroundCalculationTimeout()

    def _refresh_diagnostic_info(self, **kwargs: Any) -> None:
        self._last_request.clear()
        self._last_request = kwargs

    def _get_response(self, url_suffix: str, **kwargs: Any) -> requests.Response:
        self._refresh_diagnostic_info(method="GET", url_suffix=url_suffix, **kwargs)
        return self.http_client.get(url_suffix, **kwargs)

    def _post_response(
        self, request: Dict, url_suffix: str, **kwargs: Any
    ) -> requests.Response:
        self._refresh_diagnostic_info(
            method="POST", request=request, url_suffix=url_suffix, **kwargs
        )
        return self.http_client.post(url_suffix, request, **kwargs)
