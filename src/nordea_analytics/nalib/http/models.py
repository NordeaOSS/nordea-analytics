from json import JSONDecodeError
from typing import Any, Optional, Dict

import requests


class AnalyticsApiResponse:
    """Class representing the Analytics API response and its properties."""

    def __init__(self, raw_response: requests.Response) -> None:
        """Create instance of AnalyticsApiResponse."""
        self.__response = raw_response
        self.__json = None

    @property
    def raw_response(self) -> requests.Response:
        """Return raw response."""
        return self.__response

    @property
    def url(self) -> str:
        """Url of request."""
        return self.raw_response.request.url

    @property
    def method(self) -> str:
        """Method of request."""
        return self.raw_response.request.method

    @property
    def request_id(self) -> str:
        """Return request ID."""
        return self.headers.get("x-request-id", "")

    @property
    def status_code(self) -> int:
        """Return request status code."""
        return self.raw_response.status_code

    @property
    def headers(self) -> Any:
        """Return collection of request headers."""
        return self.raw_response.headers

    @property
    def has_data(self) -> bool:
        """Check if data is presented in response."""
        return "data" in self.json()

    @property
    def data(self) -> Optional[Dict]:
        """Return data in case of successful response."""
        return self.json().get("data", None)

    @property
    def data_response(self) -> Optional[Dict]:
        """Return response in case of successful response."""
        return self.json().get("data", {}).get("response", None)

    @property
    def error(self) -> Optional[str]:
        """Return error in case of unsuccessful response."""
        return self.json().get("error", None)

    @property
    def error_code(self) -> Optional[str]:
        """Return error code in case of unsuccessful response."""
        return self.json().get("error_code", None)

    @property
    def error_description(self) -> Optional[str]:
        """Return error description in case of unsuccessful response."""
        try:
            json_response = self.json()

            # In case IBM gateway reject the request:
            if "moreInformation" in json_response:
                return json_response.get("moreInformation")

            # In case we got it from backend service
            if "error_description" in json_response:
                return json_response.get("error_description")

            return None
        except (JSONDecodeError, ValueError):
            return f"{self.status_code}: {self.raw_response.text}"

    def json(self) -> Any:
        """Returns the json-encoded content of a response, if any."""
        if self.__json is None:
            self.__json = self.raw_response.json()
        return self.__json

    @property
    def diagnostic(self) -> Dict:
        """Return a response diagnostic information."""
        params = (
            getattr(self.raw_response.request, "params", None)
            or getattr(self.raw_response.request, "body", None)
            or {}
        )
        if isinstance(params, bytes):
            params = params.decode("utf-8")

        return {
            "request": {"url": self.url, "method": self.method, "params": params},
            "response": {
                "error": self.error or "",
                "error_code": self.error_code or "",
                "error_description": self.error_description or "",
                "headers": [f"{k}:{v}" for k, v in self.headers.items()],
                "text": self.raw_response.text,
                "status_code": self.status_code,
            },
        }
