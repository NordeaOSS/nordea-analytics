import datetime
from threading import Event
from typing import Any, Iterator, List
import urllib.parse

import requests

from nordea_analytics.nalib.http.core import RestApiHttpClient
from nordea_analytics.nalib.live_keyfigures.core import HttpStreamIterator

URL_SUFFIX = "bonds/live-keyfigures"


class OpenBankingHttpStreamIterator(HttpStreamIterator):
    """Contain methods to create iterator for request/response live keyfigures streaming."""

    def __init__(
        self,
        http_client: RestApiHttpClient,
        interval_between_requests_sec: int = 5,
        stream_suffix: str = URL_SUFFIX,
    ) -> None:
        """Constructs an :class:`OpenBankingHttpStreamIterator <OpenBankingHttpStreamIterator>`.

        Args:
            http_client: instance of RestApiHttpClient which will perform HTTP requests.
            interval_between_requests_sec: minimal interval between requests.
            stream_suffix: url where the HTTP stream is located.
        """
        self.http_client = http_client
        self.interval_in_sec = interval_between_requests_sec
        self.stream_suffix: str = stream_suffix
        self._last_request = datetime.datetime.min

    def __enter__(self) -> HttpStreamIterator:
        """Entry to the body of the 'with' statement."""
        return self

    def __exit__(self, exc_type: None, exc_val: None, exc_tb: None) -> None:
        """Exit from the body of the 'with' statement."""
        pass

    def stream(self, bonds: List) -> Iterator[Any]:
        """Return Iterator for request/response HTTP streaming that request updates from a server within an interval.

        Args:
            bonds: List of Bonds to request updates for.

        Raises:
            StopIteration: when iterator failed to proceed response from server.

        Yields:
            Iterate over HTTP stream.
        """
        params = {"bonds": str.join(",", bonds)}
        stream_url = f"{self.stream_suffix}?{urllib.parse.urlencode(params)}"
        sleep_event = Event()
        sleep_event.clear()

        while True:
            # Sleep if too many requests per second
            interval_between_requests = datetime.datetime.utcnow() - self._last_request
            if interval_between_requests.total_seconds() < self.interval_in_sec:
                sleep_event.wait(
                    self.interval_in_sec - interval_between_requests.total_seconds()
                )
            self._last_request = datetime.datetime.utcnow()

            response = self.http_client.get(stream_url)
            try:
                response.raise_for_status()
            except requests.models.HTTPError as e:
                raise StopIteration("Failed to read from remote server") from e

            yield response.text
