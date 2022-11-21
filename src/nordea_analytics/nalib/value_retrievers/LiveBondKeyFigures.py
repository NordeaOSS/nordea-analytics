import json
import math
from typing import Any, Dict, Iterator, List, Union

import numpy as np
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
    RequestMethod,
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
            LiveBondKeyFigureName,
            str,
            List[LiveBondKeyFigureName],
            List[str],
            List[Union[LiveBondKeyFigureName, str]],
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
            if type(keyfigure) == LiveBondKeyFigureName
            else keyfigure.lower()
            for keyfigure in _keyfigures
        ]

        self.keyfigures_original = _keyfigures
        self._as_df = as_df
        self._stream_iterator = Iterator[Any]
        self._data = self.get_live_key_figure_response

    @property
    def get_live_key_figure_response(self) -> List[Dict]:
        """Returns the latest available live key figures from cache."""
        json_response: List[Any] = []
        for request_dict in self.request:
            json_response += self._client.get_response(
                request_dict, self.url_suffix, RequestMethod.Get
            )["keyfigure_values"]

        return json_response

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
    def request(self) -> List[Dict]:
        """Request list of dictionaries for a given set of bonds, key figures and calc date."""
        if len(self.symbols) > config["max_bonds"]:
            split_symbols = np.array_split(
                self.symbols, math.ceil(len(self.symbols) / config["max_bonds"])
            )
            request_dict = [{"bonds": list(symbols)} for symbols in split_symbols]
        else:
            request_dict = [{"bonds": self.symbols}]

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        results: Dict = {}
        for values in self._data:

            results = results | filter_keyfigures(
                values, self.keyfigures, self.keyfigures_original
            )

        return results

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        return to_data_frame(self.to_dict())

    def _response_decorator(self, json_payload: dict) -> Any:
        json_payload = parse_live_keyfigures_json(
            json_payload, self.keyfigures, self.keyfigures_original
        )
        if self._as_df:
            return to_data_frame(json_payload)

        return json_payload
