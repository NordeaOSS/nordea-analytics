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
    """The server can't proceed the background calculation request."""

    pass


class BackgroundCalculationTimeout(Exception):
    """Throw when the time allotted for a background calculation has expired."""

    pass
