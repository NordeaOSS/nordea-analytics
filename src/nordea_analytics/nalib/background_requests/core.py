import abc
from typing import Dict, Any

from nordea_analytics.nalib.data_retrieval_client import validation
from nordea_analytics.nalib.exceptions import AnalyticsWarning, CustomWarning
from nordea_analytics.nalib.http.core import RestApiHttpClient


class BackgroundRequestsClient(metaclass=abc.ABCMeta):
    """A client for making API background requests to the Nordea Analytics REST API and handling responses."""

    def __init__(self, http_client: RestApiHttpClient) -> None:
        """Constructs a :class:`BackgroundRequestsClient`.

        Args:
            http_client: The HTTP client used to make requests.
        """
        self.http_client = http_client

    @abc.abstractmethod
    def get_calculation_asynchronous(self, request: Dict, url_suffix: str) -> Dict:
        """Sends a request for a bulk background calculation and retrieves the response.

        Args:
            request (Dict): The request data in dictionary form.
            url_suffix (str): The URL suffix for the given method.

        Returns:
            The response data in JSON format.

        This function sends a POST request for a bulk background calculation, verifies that the response is valid,
        proceeds with the background jobs, and checks for errors in the response.
        """
        pass

    @abc.abstractmethod
    def get_response_asynchronous(self, request: Dict, url_suffix: str) -> Dict:
        """Sends a request for a background calculation and retrieves the response.

        Args:
            request (Dict): The request data in dictionary form.
            url_suffix (str): The URL suffix for the given method.

        Returns:
            The response data in JSON format.

        This function sends a POST request for a background calculation, verifies that the response is valid,
        proceeds with the background job, and checks for errors in the response.
        """
        pass

    def _get_jobs_results(
        self, valid_jobs: Dict[str, str], request_id: str | None
    ) -> Dict[str, Any]:
        headers = {}
        if request_id:
            headers = {"X-Request-ID-Override": request_id}
        api_response = self.http_client.post(
            url_suffix="jobs",
            json={"jobs": list(valid_jobs.keys())},
            headers=headers,
        )

        results = {}
        api_responses = api_response.json().get("data", [])
        for calculation_response in api_responses:
            response = calculation_response["response"]
            validation.raise_warnings_for(response, "failed_calculation")

            info = calculation_response["info"]
            if info["job_id"] not in valid_jobs:
                CustomWarning("Incorrect API response", AnalyticsWarning)
                continue

            results[valid_jobs[info["job_id"]]] = response
        return results
