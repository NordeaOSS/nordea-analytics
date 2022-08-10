import json
from typing import Any, Dict, Iterator, List, Union

import pandas as pd

from nordea_analytics.key_figure_names import (
    LiveBondKeyFigureName,
)
from nordea_analytics.nalib.data_retrieval_client import DataRetrievalServiceClient
from nordea_analytics.nalib.live_keyfigures.parsing import (
    filter_keyfigures,
    parse_live_keyfigures_json,
    to_data_frame,
)
from nordea_analytics.nalib.util import (
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class LiveBondKeyFigures(ValueRetriever):
    """Retrieves and reformats calculated live bond key figure."""

    def __init__(
        self,
        symbols: Union[str, List[str]],
        client: DataRetrievalServiceClient,
        keyfigures: Union[
            List[LiveBondKeyFigureName], List[str], LiveBondKeyFigureName, str
        ],
        as_df: bool,
    ) -> None:
        """Initialization of class.

        Args:
            symbols: ISIN or name of bonds that should be retrieved live
            client: LiveDataRetrivalServiceClient
            keyfigures: List of bond key figures which should be streamed.
                Can be a list of LiveBondKeyFigureNames or string.
            as_df: if True, the results are represented
                as pd.DataFrame, else as dictionary
        """
        super(LiveBondKeyFigures, self).__init__(client)
        self.symbols: List = [symbols] if isinstance(symbols, str) else symbols
        _keyfigures: List = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        self.keyfigures: List = [
            convert_to_variable_string(keyfigure, LiveBondKeyFigureName)
            for keyfigure in _keyfigures
        ]
        self._as_df = as_df
        self._stream_iterator = Iterator[Any]
        self._data = self._client.get_response(self.request, self.url_suffix)[
            "keyfigure_values"
        ]

    @property
    def stream_keyfigures(self) -> Iterator[Any]:
        """Returns the stream listener which controls the live stream."""
        for stream_chunk in self._client.get_live_streamer().stream(self.symbols):
            json_payload = json.loads(stream_chunk)
            yield self._response_decorator(json_payload)

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["live_bond_key_figures"]

    @property
    def request(self) -> Dict:
        """Request dictionary for a given set of bonds, key figures and calc date."""
        request_dict = {"bonds": str.join(",", self.symbols)}
        return request_dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        return to_data_frame(self.to_dict())

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        results: Dict = {}
        for values in self._data:
            results = results | filter_keyfigures(values, self.keyfigures)
        return results

    def _response_decorator(self, json_payload: dict) -> Any:
        json_payload = parse_live_keyfigures_json(json_payload, self.keyfigures)
        if self._as_df:
            return to_data_frame(json_payload)

        return json_payload
