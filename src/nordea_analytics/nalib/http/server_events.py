import time
from typing import Iterator, List, Self

import requests

from nordea_analytics.nalib.exceptions import CustomWarningCheck
from nordea_analytics.nalib.http.core import RestApiHttpClient

FIELD_SEPARATOR = ":"
URL_SUFFIX = "bonds/live-keyfigures-stream"


class ServerEventMessage(object):
    """Processes messages from the live streaming service."""

    def __init__(self) -> None:
        """Initialization of class."""
        self.event_type = ""
        self.data = ""


def _non_empty_lines(chunk: bytes, encoding: str = "utf-8") -> Iterator[bytes]:
    """Split byte chunk into lines."""
    lines = chunk.splitlines()
    for line in lines:
        line = line.decode(encoding).strip()  # type: ignore
        if len(line) != 0 and not line.isspace():
            yield line


def _index_of(s: str, sub: str) -> int:
    try:
        return s.index(sub)
    except ValueError:
        return -1


def _left_part(s: str, sep: str) -> str:
    i = _index_of(s, sep)
    return s[:i] if i > -1 else s


def _right_part(s: str, sep: str) -> str:
    i = _index_of(s, sep)
    return s[i + 1 :] if i > -1 else s


def _process_line_to_msg(lines: List) -> ServerEventMessage:
    """Transform byte lines into ServiceEventMessage."""
    msg = ServerEventMessage()
    for line in lines:
        label = _left_part(line, FIELD_SEPARATOR).lstrip()
        data = _right_part(line, FIELD_SEPARATOR).lstrip()
        if len(data) > 0 and data[0] == " ":
            data = data[1:]
        if label == "event":
            msg.event_type = data
        elif label == "data":
            msg.data = data

    return msg


def _create_message(chunk_data: bytes) -> ServerEventMessage:
    all_lines = list(_non_empty_lines(chunk_data))
    server_msg = _process_line_to_msg(all_lines)
    return server_msg


class ServerEventsStreamer:
    """Contain methods to create iterator for Server-Sent Events HTTP Stream that receive updates from a server."""

    def __init__(
        self,
        http_client: RestApiHttpClient,
        stream_suffix: str = URL_SUFFIX,
    ) -> None:
        """Constructs a :class:`NordeaHttpStreamIterator <NordeaHttpStreamIterator>`.

        Args:
            http_client: instance of RestApiHttpClient which will perform HTTP requests.
            stream_suffix: url where the HTTP stream is created.
        """
        self.http_client = http_client
        self.stream_suffix = stream_suffix
        self._stream_url: str = ""
        self._stream: requests.Response = None

    def __enter__(self) -> Self:
        """Entry to the body of the 'with' statement."""
        return self

    def __exit__(self, exc_type: None, exc_val: None, exc_tb: None) -> None:
        """Exit from the body of the 'with' statement."""
        if self._stream is not None:
            self._stream.close()

    def stream(self) -> Iterator[ServerEventMessage]:
        """Return Iterator for Server-Sent Events HTTP Stream that receive updates from a server.

        Yields:
            Iterate over HTTP stream.
        """
        self._stream_url = self.stream_suffix
        data = b""
        self._stream = self._start_http_stream()
        timer = time.time()
        for chunk in self._stream.iter_content():
            for line in chunk.splitlines(True):
                data += line
                if data.endswith((b"\r\r", b"\n\n", b"\r\n\r\n")):
                    server_msg = _create_message(data)
                    data = b""
                    if server_msg.event_type != "":
                        timer = time.time()
                        yield server_msg
                    else:
                        if time.time() - timer > 10 * 60:
                            CustomWarningCheck.live_key_figure_no_data_closing_down_warning()
                            self._stream.__exit__()

    def _start_http_stream(self) -> requests.Response:
        try:
            response = self.http_client.get(
                url_suffix=self._stream_url,
                stream=True,
                headers={"Accept": "text/event-stream"},
                verify=True,
            )

            response.raw_response.raise_for_status()
            return response.raw_response
        except requests.models.HTTPError:
            response.raw_response.close()
            raise
