# -*- coding: utf-8 -*-

"""Server Events client for DataRetrieval Service."""
from threading import Event, Thread
from typing import Any, Callable, Dict, Generator, Iterator, List

import requests
from requests import Response
from requests.auth import HTTPBasicAuth

_FIELD_SEPARATOR = ":"


class ServerEventMessage(object):
    """Processes messages from the live streaming service."""

    def __init__(self) -> None:
        """Initialization of class."""
        self.eventId = -1
        self.channel = ""
        self.data = ""
        self.selector = ""
        self.json = ""
        self.op = ""
        self.target = ""
        self.cssSelector = ""
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

    e.selector = _left_part(e.data, " ")
    if _index_of(e.data, "@") >= 0:
        e.channel = _left_part(e.selector, "@")
        e.selector = _right_part(e.selector, "@")

    e.json = _right_part(e.data, " ")

    if not len(e.selector) == 0:
        if _index_of(e.selector, ".") == -1:
            raise Exception(f"Invalid selector: {e.selector}")

        e.op = _left_part(e.selector, ".")
        sanitize_selector = _right_part(e.selector, ".").replace("%20", " ")
        e.target = _right_part(sanitize_selector, "$")
        if e.op == "cmd":
            if e.target == "onConnect":
                e.json_obj = json.loads(e.json)
            elif e.target == "onUpdate":
                e.json_obj = json.loads(e.json)
            elif e.target == "onHeartbeat":
                e.json_obj = json.loads(e.json)


def process_line_to_msg(lines: List) -> ServerEventMessage:
    """Transform byte lines into ServiceEventMessage."""
    msg = ServerEventMessage()
    for line in lines:
        label = _left_part(line, _FIELD_SEPARATOR).lstrip()
        data = _right_part(line, _FIELD_SEPARATOR).lstrip()
        if len(data) > 0 and data[0] == " ":
            data = data[1:]
        if label == "id":
            msg.eventId = int(data)
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


class InternalStreamListener(StreamListener):
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
        super(InternalStreamListener, self).__init__(baseurl, isins, update_method)
        self._live_stream: Any = None
        self._heartbeat_url = ""
        self._subscription_id = ""
        self._sleep_event = Event()
        self._timeout = 0.0
        self._event_stream: Response
        self._heartbeat_stream_thread = Thread(target=self._send_heartbeat)
        self._heartbeat_stream_thread.daemon = True
        self.auth = auth

    def __enter__(self) -> "StreamListener":
        """Enter the stream."""
        self.start()
        return self

    def __exit__(self, exc_type: None, exc_val: None, exc_tb: None) -> None:
        """Exit the stream. Nothing here, so that dashboards work."""

    def _send_heartbeat(self) -> None:
        while not self._sleep_event.wait(self._timeout):
            requests.get(self._heartbeat_url, verify=True)

    def _start_stream(self) -> Generator[None, None, None]:
        for chunk_data in _read(self._event_stream):
            if not self.is_working:
                raise StopIteration("Live feed has been stopped.")
            all_lines = list(non_empty_lines(chunk_data))
            server_msg = process_line_to_msg(all_lines)
            to_json(server_msg)
            if server_msg.target == "onConnect":
                self._heartbeat_url = server_msg.json_obj["heartbeatUrl"]
                self._subscription_id = server_msg.json_obj["id"]
                self._timeout = int(server_msg.json_obj["heartbeatIntervalMs"]) / 1000
                if not self._heartbeat_stream_thread.is_alive():
                    self._heartbeat_stream_thread.start()
            elif server_msg.target == "UpdateLiveKeyfigureDto":
                yield self.update_method(server_msg.json)

    def run(self) -> str:
        """Keeps the stream running."""
        return next(self._live_stream)

    def start(self) -> None:
        """Callable method to start the stream."""
        self.url = f'{self.baseurl}?channels={",".join(self.isins)}'
        if not self.is_working:
            self._sleep_event.clear()
            self._event_stream = requests.get(
                self.url,
                stream=True,
                headers={"Accept": "text/event-stream"},
                verify=True,
                auth=self.auth,
            )
            self._live_stream = self._start_stream()
        self.is_working = True

    def stop(self) -> None:
        """Callable method to stop the stream."""
        if self.is_working:
            self.is_working = False
            self._event_stream.close()
            self._sleep_event.set()
            self._heartbeat_stream_thread.join()


class ExternalStreamListener(StreamListener):
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
        super(ExternalStreamListener, self).__init__(baseurl, isins, update_method)
        self._sleep_event = Event()
        self._sleep_event.clear()
        self._timeoutInSec = 10
        self._live_stream: Iterator[Any] = self.empty_iterator()
        self.get_response: Callable = get_response

    def __enter__(self) -> "StreamListener":
        """Enter the stream."""
        self.run()
        return self

    def __exit__(self, exc_type: None, exc_val: None, exc_tb: None) -> None:
        """Exit the stream."""
        self.stop()

    def run(self) -> str:
        """Keeps the stream running."""
        if hasattr(self, "_live_stream"):
            self.start()
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
        self._live_stream = self._stream_iter()

    def _stream_iter(self) -> Generator[str, None, None]:
        """Iterates the the request to continue the stream."""
        while self.is_working:
            response = self.get_response({}, self.url)
            if not response.ok:
                break

            yield self.update_method(response.json())

            if not self.is_working or self._sleep_event.wait(self._timeoutInSec):
                self.is_working = False
                break

    @staticmethod
    def empty_iterator() -> Iterator:
        """Empty iterator to define self._live_stream variable."""
        yield from ()
