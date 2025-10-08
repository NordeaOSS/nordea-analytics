import time
from typing import Dict, List, Literal

from nordea_analytics.nalib.background_requests.core import BackgroundRequestsClient
from nordea_analytics.nalib.data_retrieval_client import validation
from nordea_analytics.nalib.data_retrieval_client.dto.background import (
    BackgroundJobResponse,
)
from nordea_analytics.nalib.data_retrieval_client.dto.background import (
    BackgroundJobStatusResponse,
)
from nordea_analytics.nalib.data_retrieval_client.dto.bulk_calculation import (
    PostBondsBulkCalculationData,
    BondsBulkCalculationStatusData,
    BondsCalculationStatusDto,
)
from nordea_analytics.nalib.exceptions import (
    BackgroundCalculationFailed,
    BackgroundCalculationTimeout,
    CustomWarning,
    AnalyticsWarning,
)
from nordea_analytics.nalib.http.core import RestApiHttpClient
from nordea_analytics.nalib.http.models import AnalyticsApiResponse


class PollingBackgroundRequestsClient(BackgroundRequestsClient):
    """A client for making API background requests to the Nordea Analytics REST API and handling responses.

    Attributes:
        http_client (RestApiHttpClient): The HTTP client used to make requests.
    """

    def __init__(self, http_client: RestApiHttpClient) -> None:
        """Constructs a :class:`PollingBackgroundRequestsClient`.

        Args:
            http_client: The HTTP client used to make requests.
        """
        super().__init__(http_client)

    def retrieve_response_asynchronous(
        self, request: Dict, url_suffix: str, method: Literal["GET", "POST"] = "GET"
    ) -> Dict:
        """Sends a request for a background calculation and retrieves the response.

        Args:
            request (Dict): The request data in dictionary form.
            url_suffix (str): The URL suffix for the given method.
            method (str): The HTTP method.

        Returns:
            The response data in JSON format.

        Raises:
            NotImplementedError: Give HTTP method is not supported.

        This function sends a GET request for a background calculation, verifies that the response is valid,
        proceeds with the background job, and checks for errors in the response.
        """
        # Step 1: get data
        if method == "GET":
            api_response = self.http_client.get(url_suffix, params=request)
        elif method == "POST":
            api_response = self.http_client.post(url_suffix=url_suffix, json=request)
        else:
            raise NotImplementedError(f"HTTP {method} is not supported.")

        # Step 2: poll server until the data will arrive
        results = self._poll_server(api_response)

        validation.raise_warnings_for(results.data_response, "failed_calculation")
        return results.data_response or {}

    def get_calculation_asynchronous(self, request: Dict, url_suffix: str) -> List:
        """Sends a request for a bulk background calculation and retrieves the response.

        Args:
            request (Dict): The request data in dictionary form.
            url_suffix (str): The URL suffix for the given method.

        Returns:
            The response data in JSON format.

        This function sends a POST request for a bulk background calculation, verifies that the response is valid,
        proceeds with the background jobs, and checks for errors in the response.
        """

        # Step 1: post data
        api_response = self.http_client.post(url_suffix, json=request)

        # Step 2: poll server until the data will arrive
        results = self._poll_server_bulk_job(api_response)

        return results

    def _poll_server(self, api_response: AnalyticsApiResponse) -> AnalyticsApiResponse:
        background_job = BackgroundJobResponse(api_response.json())
        timeout_seconds = 60 * 8
        end_time = time.monotonic() + timeout_seconds
        while time.monotonic() < end_time:
            api_response = self.http_client.get(
                url_suffix=f"job/{background_job.id}",
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

    def _poll_server_bulk_job(self, api_response: AnalyticsApiResponse) -> List:
        bulk_data = PostBondsBulkCalculationData(api_response.data)

        # MG: validate errors:
        calculation_statuses = [
            self._any_valid_calculation(bulk_data.standard),
            self._any_valid_calculation(bulk_data.advanced),
            self._any_valid_calculation(bulk_data.repo),
            self._any_valid_calculation(bulk_data.horizon),
        ]

        if all(not valid for valid in calculation_statuses):
            return []

        timeout_seconds = 60 * 8
        end_time = time.monotonic() + timeout_seconds
        while time.monotonic() < end_time:
            api_response = self.http_client.get(
                url_suffix=f"bulk-job/{bulk_data.id}",
                headers={"X-Request-ID-Override": api_response.request_id},
            )
            poll_response = BondsBulkCalculationStatusData(api_response.data)
            state = poll_response.state  # type: ignore
            if state == "completed" or state == "failed":
                return self._retrieve_background_job_results(api_response, bulk_data)

            if state in ("new", "processing", "rescheduled"):
                time.sleep(0.2 + (0.2 * len(poll_response.underlying_jobs)) // 10)
                continue

        raise BackgroundCalculationTimeout()

    def _any_valid_calculation(self, status: BondsCalculationStatusDto) -> bool:
        if (
            status is None
            or status.calculations is None
            or len(status.calculations) == 0
        ):
            return False

        any_valid_calculation = False
        for calculation in status.calculations:
            if calculation.status_code == 200:
                any_valid_calculation = True
            else:
                error_message = f'{calculation.symbol}: {calculation.error or "Failed to calculate"}'
                CustomWarning(error_message, AnalyticsWarning)

        return any_valid_calculation

    def _retrieve_background_job_results(
        self, api_response: AnalyticsApiResponse, data: PostBondsBulkCalculationData
    ) -> List:
        # MG: check status
        valid_jobs = {}
        valid_jobs.update(self._validate_and_get_symbol_map(data.standard))
        valid_jobs.update(self._validate_and_get_symbol_map(data.advanced))
        valid_jobs.update(self._validate_and_get_symbol_map(data.horizon))
        valid_jobs.update(self._validate_and_get_symbol_map(data.repo))

        results = self._get_jobs_results(valid_jobs, api_response.request_id)

        return results

    def _validate_and_get_symbol_map(
        self, data: BondsCalculationStatusDto
    ) -> Dict[str, str]:
        symbol_map = {}
        if (
            data is not None
            and data.calculations is not None
            and len(data.calculations) > 0
        ):
            for calculation in data.calculations:
                if calculation.status_code != 200:
                    error_message = (
                        calculation.error or f"Failed to calculate {calculation.symbol}"
                    )
                    CustomWarning(error_message, AnalyticsWarning)
                    continue

                symbol_map[calculation.job_id] = calculation.symbol

        return symbol_map
