from typing import Any
import warnings


class HttpClientImproperlyConfigured(Exception):
    """The http service is somehow improperly configured."""

    pass


class ApiServerUnauthorized(Exception):
    """The server can't authorize request."""

    pass


class ApiServerError(Exception):
    """Basic exception for all server errors."""

    def __init__(self, error_id: str, error_description: str) -> None:
        """Common base class for all Analytics API Server exceptions."""
        self.error_id = error_id
        super(ApiServerError, self).__init__(error_description)

    def __str__(self) -> str:
        """Return str(self)."""
        return f"Error code: {self.error_id} -> {super(ApiServerError, self).__str__()}"


class BackgroundCalculationFailed(ApiServerError, Exception):
    """The server can't process the background calculation request."""

    pass


class BackgroundCalculationTimeout(Exception):
    """Throw when the time allotted for a background calculation has expired."""

    pass


class AnalyticsResponseError(Exception):
    """Throw when response from Analytics API is not as expected."""

    pass


class AnalyticsInputError(Exception):
    """Throw when input to Analytics API is not as expected."""

    pass


class AnalyticsWarning(Warning):
    """Category which is used for Analytics warnings only."""

    pass


class CustomWarning(Warning):
    """Throw custom instead of standard warning to indicate warning came from Analytics API."""

    def __init__(self, message: str, category: Any) -> None:
        """Create new instance of class.

        Args:
            message: warning message.
            category: warning category.
        """
        self.message = message
        warnings.warn(self.message, category=category)


class CustomWarningCheck:
    """Class for containing custom warning messages."""

    @staticmethod
    def curve_not_retrieved_warning(response: dict, curve: str) -> None:
        """Throw warning when curve time series does not return anything."""
        if len(response["curve"]["curve"]["values"]) == 0:
            message = curve + " could not be retrieved."
            CustomWarning(message, AnalyticsWarning)

    @staticmethod
    def curve_time_series_not_retrieved_warning(response: dict, curve: str) -> None:
        """Throw warning when curve time series does not return anything."""
        if "timeseries" in response and len(response["timeseries"]) == 0:
            message = curve + " could not be retrieved."
            CustomWarning(message, AnalyticsWarning)

    @staticmethod
    def post_response_not_retrieved_warning(error: Exception, symbol: str) -> None:
        """Throw warning when post response throws exception to ensure result from remaining bonds is returned."""
        error_code = error.error_id if isinstance(error, ApiServerError) else ""
        if len(error.args) > 0 and "Failed to retrieve bond.":
            message = f"{symbol} could not be retrieved, {error.args[0]} Error code: {error_code}"
            CustomWarning(message, AnalyticsWarning)
