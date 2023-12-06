"""Contains objects which are representing the errors."""


class ClientHttpError(Exception):
    """Basic exception for all server errors."""

    def __init__(self, error_id: str, error_description: str) -> None:
        """Common base class for all Analytics API Server exceptions."""
        self.error_id = error_id
        super(ClientHttpError, self).__init__(error_description)

    def __str__(self) -> str:
        """Return str(self)."""
        return f"{self.__doc__} Error code: {self.error_id}. Error description: {super(ClientHttpError, self).__str__()}"


class BadRequestError(ClientHttpError):
    """The Analytics API cannot process the request due to client request error."""

    def __init__(self, error_id: str, error_description: str) -> None:
        """Create new instance of BadRequestError."""
        super().__init__(error_id, error_description)


class UnauthorizedRequestError(ClientHttpError):
    """Authentication is required but has failed or has not yet been provided."""

    def __init__(self, error_id: str, error_description: str) -> None:
        """Create new instance of UnauthorizedRequestError."""
        super().__init__(error_id, error_description)


class ForbiddenRequestError(ClientHttpError):
    """Analytics API server is refusing action due to insufficient privileges."""

    def __init__(self, error_id: str, error_description: str) -> None:
        """Create new instance of ForbiddenRequestError."""
        super().__init__(error_id, error_description)


class NotFoundRequestError(ClientHttpError):
    """The requested resource could not be found."""

    def __init__(self, error_id: str, error_description: str) -> None:
        """Create new instance of NotFoundRequestError."""
        super().__init__(error_id, error_description)


class UnknownClientError(ClientHttpError):
    """The error caused by the client request."""

    def __init__(self, error_id: str, error_description: str) -> None:
        """Create new instance of UnknownClientError."""
        super().__init__(error_id, error_description)
