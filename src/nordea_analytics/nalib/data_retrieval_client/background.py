import time
from typing import Dict

from nordea_analytics.nalib.data_retrieval_client import validation
from nordea_analytics.nalib.data_retrieval_client.core import BaseDataRetrievalClient
from nordea_analytics.nalib.data_retrieval_client.dto.background import (
    BackgroundJobResponse,
)
from nordea_analytics.nalib.data_retrieval_client.dto.background import (
    BackgroundJobStatusResponse,
)
from nordea_analytics.nalib.exceptions import (
    AnalyticsResponseError,
    BackgroundCalculationFailed,
    BackgroundCalculationTimeout,
)
from nordea_analytics.nalib.http.core import RestApiHttpClient
from nordea_analytics.nalib.http.models import AnalyticsApiResponse
from nordea_analytics.nalib.util import RequestMethod


class BackgroundRequestsClient(BaseDataRetrievalClient):
    """A client for making API background requests to the Nordea Analytics REST API and handling responses.

    Attributes:
        http_client (RestApiHttpClient): The HTTP client used to make requests.
    """

    def __init__(self, http_client: RestApiHttpClient) -> None:
        """Constructs a :class:`BackgroundRequestsClient <BackgroundRequestsClient>`.

        Args:
            http_client: The HTTP client used to make requests.
        """
        super().__init__(http_client)

    def get_response_asynchronous(self, request: Dict, url_suffix: str) -> Dict:
        """Sends a request for a background calculation and retrieves the response.

        Args:
            request (Dict): The request data in dictionary form.
            url_suffix (str): The URL suffix for the given method.

        Returns:
            The response data in JSON format.

        Raises:
            AnalyticsResponseError: If an error is found, this function raises an 'AnalyticsResponseError' exception.

        This function sends a POST request for a background calculation, verifies that the response is valid,
        proceeds with the background job, and checks for errors in the response.
        """

        # Step 1: post data
        api_response = self.send(request, url_suffix, RequestMethod.Post)
        if not self._is_background_response(api_response):
            raise AnalyticsResponseError(
                f"Got invalid background job response schema. Request id: {api_response.request_id}"
            )

        # Step 2: poll server until the data will arrive
        api_response = self._poll_server(api_response)

        # Step 3: validate and return the response data
        validation.validate_response(api_response)
        return api_response.data_response  # type: ignore

    def _poll_server(self, api_response: AnalyticsApiResponse) -> AnalyticsApiResponse:
        background_job = BackgroundJobResponse(api_response.json())
        timeout_seconds = 60 * 8
        end_time = time.monotonic() + timeout_seconds
        while time.monotonic() < end_time:
            api_response = self.send(
                request={},
                url_suffix=f"job/{background_job.id}",
                request_method=RequestMethod.Get,
                headers={"X-Request-ID-Override": api_response.request_id},
            )
            poll_response = BackgroundJobStatusResponse(api_response.data)
            state = poll_response.info.state  # type: ignore

            if state == "completed":
                return api_response

            if state == "failed":
                error_description = "Background job failed to proceed."

                if api_response.error_description is not None:
                    error_description += f" {api_response.error_description}"
                raise BackgroundCalculationFailed(
                    error_id=api_response.request_id,
                    error_description=error_description,
                )

            if state in ("new", "processing", "rescheduled"):
                time.sleep(0.2)
                continue

        raise BackgroundCalculationTimeout()

    def _is_background_response(self, api_response: AnalyticsApiResponse) -> bool:
        return "id" in api_response.json() and "info" in api_response.json()

    def _get_error_description(self, api_response: AnalyticsApiResponse) -> str:
        if "error_description" in api_response.data_response:  # type: ignore[operator]
            return api_response.data_response["error_description"]  # type: ignore[index]
        else:
            return "Background job failed to proceed"
