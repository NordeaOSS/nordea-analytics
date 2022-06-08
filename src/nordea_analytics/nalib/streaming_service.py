# -*- coding: utf-8 -*-

"""Server Events client for DataRetrieval Service."""
from threading import Event
from typing import Any, Callable, Dict, Generator, Iterator, List
import warnings

import requests
from requests import Response
from requests.auth import HTTPBasicAuth

from nordea_analytics.nalib import auth

_FIELD_SEPARATOR = ":"


class ServerEventMessage(object):
    """Processes messages from the live streaming service."""

    def __init__(self) -> None:
        """Initialization of class."""
        self.eventId = -1
        self.eventType = ""
        self.channel = ""
        self.data = ""
        self.json = ""
        self.json_obj: Dict[Any, Any] = {}


def _read(event_source: Response) -> Generator[bytes, None, None]:
    """Read the incoming event source stream and yield event chunks."""
    data = b""
    try:
        for chunk in event_source:
            for line in chunk.splitlines(True):
                data += line
                if data.endswith((b"\r\r", b"\n\n", b"\r\n\r\n")):
                    yield data
                    data = b""
        if data:
            yield data
    except Exception as e:
        print(e)


def non_empty_lines(chunk: bytes, encoding: str = "utf-8") -> Generator:
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


def to_json(e: ServerEventMessage) -> None:
    """Transform ServiceEventMessage to json format."""
    import json

    e.json = e.data
    e.json_obj = json.loads(e.json)


def process_line_to_msg(lines: List) -> ServerEventMessage:
    """Transform byte lines into ServiceEventMessage."""
    msg = ServerEventMessage()
    for line in lines:
        label = _left_part(line, _FIELD_SEPARATOR).lstrip()
        data = _right_part(line, _FIELD_SEPARATOR).lstrip()
        if len(data) > 0 and data[0] == " ":
            data = data[1:]
        if label == "event":
            msg.eventType = data
        elif label == "data":
            msg.data = data

    return msg


class StreamListener(object):
    """Parent call for live stream."""

    def __init__(self, baseurl: str, isins: List, update_method: Callable) -> None:
        """Initialization of class.

        Args:
            baseurl: url which retrieves the streamed data.
            isins: ISINs which should be streamed.
            update_method: Reference to callable method where the streamed data is
                transformed into a presentable format.
        """
        self.is_working = False
        self.baseurl = baseurl.rstrip("/")
        self.url = ""
        self.isins = isins
        self.update_method = update_method


class LiveKeyfigureStreamListener(StreamListener):
    """Retrieves and Controls the internal live stream."""

    def __init__(
        self, baseurl: str, isins: List, update_method: Callable, auth: HTTPBasicAuth
    ) -> None:
        """Initialization of class.

        Args:
            baseurl: url which retrieves the streamed data.
            isins: ISINs which should be streamed.
            update_method: Reference to callable method where the streamed data is
                transformed into a presentable format.
            auth: Authentication to service if needed.
        """
        super(LiveKeyfigureStreamListener, self).__init__(baseurl, isins, update_method)
        self._live_stream: Any = None
        self._subscription_id = ""
        self._http_stream: Response = None
        self.auth = auth

    def __enter__(self) -> "StreamListener":
        """Enter the stream."""
        self.start()
        return self

    def __exit__(self, exc_type: None, exc_val: None, exc_tb: None) -> None:
        """Exit the stream. Nothing here, so that dashboards work."""

    def _iterate_stream(self) -> Generator[None, None, None]:
        for chunk_data in _read(self._http_stream):
            if not self.is_working:
                raise StopIteration("Live feed has been stopped.")
            all_lines = list(non_empty_lines(chunk_data))
            server_msg = process_line_to_msg(all_lines)
            to_json(server_msg)
            if server_msg.eventType == "connected":
                continue
            elif server_msg.eventType == "UpdateLiveKeyfigureDto":
                yield self.update_method(server_msg.json)

    def run(self) -> str:
        """Keeps the stream running."""
        return next(self._live_stream)

    def start(self) -> None:
        """Callable method to start the stream."""
        self.url = f'{self.baseurl}?channels={",".join(self.isins)}'
        if not self.is_working:
            self._http_stream = self._start_http_stream()
            self._live_stream = self._iterate_stream()
        self.is_working = True

    def _start_http_stream(self) -> Response:
        auth_cookies = auth.authenticate()
        if auth_cookies is None:
            raise ValueError("Authentication is not supported")

        response = requests.get(
            self.url,
            stream=True,
            headers={"Accept": "text/event-stream"},
            verify=True,
            auth=self.auth,
            cookies=auth_cookies,
        )
        response.raise_for_status()
        return response

    def stop(self) -> None:
        """Callable method to stop the stream."""
        if self.is_working:
            self.is_working = False
            self._http_stream.close()


class LiveKeyfigureListener(StreamListener):
    """Retrieves and Controls the external live stream."""

    def __init__(
        self, baseurl: str, isins: List, update_method: Callable, get_response: Callable
    ) -> None:
        """Initialization of class.

        Args:
            baseurl: url which retrieves the streamed data.
            isins: ISINs which should be streamed.
            update_method: Reference to callable method where the streamed data is
                transformed into a presentable format.
            get_response: get response function from data_retrieval_client which
                handles requests in the right way for internal and external users.
        """
        super(LiveKeyfigureListener, self).__init__(baseurl, isins, update_method)
        self._sleep_event = Event()
        self._sleep_event.clear()
        self._timeoutInSec = 10
        self._live_stream: Iterator[Any] = self.empty_iterator()
        self.get_response: Callable = get_response
        self.warnings_sent: bool = False

    def __enter__(self) -> "StreamListener":
        """Enter the stream."""
        self.start()
        return self

    def __exit__(self, exc_type: None, exc_val: None, exc_tb: None) -> None:
        """Exit the stream. Nothing here, so that dashboards work."""
        pass

    def run(self) -> str:
        """Keeps the stream running."""
        self._live_stream = self._stream_iter()
        return next(self._live_stream)

    def stop(self) -> None:
        """Callable method to stop the stream."""
        self.is_working = False
        self._sleep_event.set()

    def start(self) -> None:
        """Callable method to start the stream."""
        self._sleep_event.clear()
        self.url = f'{self.baseurl}?bonds={",".join(self.isins)}'
        self.is_working = True

    def _stream_iter(self) -> Generator[str, None, None]:
        """Iterates the the request to continue the stream."""
        while self.is_working:
            response = self.get_response({}, self.url)
            if not response.ok:
                break

            self.send_warning(response.json())
            yield self.update_method(response.json())

            if not self.is_working or self._sleep_event.wait(self._timeoutInSec):
                self.is_working = False
                break

    def send_warning(self, json_response: dict) -> None:
        """Handle data when warnings should be sent."""
        if not self.warnings_sent:
            if json_response["data"]["data_not_available"]:
                warnings.warn(
                    "Live data not available for "
                    + ",".join(json_response["data"]["data_not_available"])
                )

            if json_response["data"]["access_restricted"]:
                warnings.warn(
                    "Access to live data restricted for "
                    + ",".join(json_response["data"]["access_restricted"])
                )

            if not json_response["data"]["keyfigure_values"]:
                raise ValueError(
                    "No data was retrieved! Please look if you have further "
                    "warning messages to identify the issue."
                )

            self.warnings_sent = True

    @staticmethod
    def empty_iterator() -> Iterator:
        """Empty iterator to define self._live_stream variable."""
        yield from ()
