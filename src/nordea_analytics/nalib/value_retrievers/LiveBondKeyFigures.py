from datetime import datetime
import json
from typing import Any, Dict, List, Union

import pandas as pd

from nordea_analytics.key_figure_names import (
    LiveBondKeyFigureName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    LiveDataRetrievalServiceClient,
)
from nordea_analytics.nalib.streaming_service import StreamListener
from nordea_analytics.nalib.util import (
    convert_to_float_if_float,
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class LiveBondKeyFigures(ValueRetriever):
    """Retrieves and reformats calculated live bond key figure."""

    def __init__(
        self,
        client: LiveDataRetrievalServiceClient,
        isins: Union[str, List[str]],
        keyfigures: Union[
            List[LiveBondKeyFigureName], List[str], LiveBondKeyFigureName, str
        ],
        _dict: Dict,
        stream: bool,
        as_df: bool,
    ) -> None:
        """Initialization of class.

        Args:
            client: LiveDataRetrivalServiceClient
            isins: ISINs of bond that should be retrieved live
            keyfigures: List of bond key figures which should be streamed.
                Can be a list of LiveBondKeyFigureNames or string.
            _dict: Dictionary with live values.
            stream: If True, the results are returned as a stream.
                    If False, the results are turned as a snap-shot of latest values
            as_df: if True, the results are represented
                as pd.DataFrame, else as dictionary
        """
        super(LiveBondKeyFigures, self).__init__(client)
        self.isins = [isins] if type(isins) == str else isins
        _keyfigures: List = keyfigures if type(keyfigures) == list else [keyfigures]
        self.keyfigures = [
            convert_to_variable_string(keyfigure, LiveBondKeyFigureName)
            for keyfigure in _keyfigures
        ]
        self.as_df = as_df
        self._data: Dict[Any, Any] = {}
        self.dict = _dict
        self.stream = stream

    def get_live_streamer(self) -> StreamListener:
        """Returns the stream listener which controls the live stream."""
        return self._client.get_live_streamer(
            self.request, self.url_suffix, self.update_live_streaming
        )

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        if self.stream:
            return config["url_suffix"]["live_bond_key_figures_stream"]
        else:
            return config["url_suffix"]["live_bond_key_figures"]

    @property
    def request(self) -> Dict:
        """Request dictionary for a given set of ISINs, key figures and calc date."""
        request_dict = {i: x for i, x in enumerate(self.isins)}
        return request_dict

    def update_live_streaming(self, results: Union[Dict, str]) -> Any:
        """Streamed data is transformed into a presentable format."""
        if type(results) == str:
            self._data = json.loads(results)
            self.to_dict_stream()
        elif type(results) == dict:
            self._data = results
            self.to_dict()

        if self.as_df:
            return self.to_df()
        else:
            return self.dict

    def to_dict_stream(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict = {}

        for key_figure_data in self._data["values"]:
            key_figure_name = key_figure_data["keyfigure"].lower()
            if key_figure_name in self.keyfigures:
                _dict[
                    LiveBondKeyFigureName(key_figure_name).name
                ] = convert_to_float_if_float(key_figure_data["value"])
                _dict["timestamp"] = str(
                    datetime.fromtimestamp(key_figure_data["timestamp"])
                )
            # return empty if kf is not available live for bond
            for _kf in self.keyfigures:
                if _kf not in [x["keyfigure"].lower() for x in self._data["values"]]:
                    _dict[LiveBondKeyFigureName(_kf).name] = ""

        self.dict[self._data["isin"]] = _dict
        return self.dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary for external stream."""
        for isin_data in self._data["data"]["keyfigure_values"]:
            _dict = {}
            for key_figure_data in isin_data["values"]:
                key_figure_name = key_figure_data["keyfigure"].lower()
                if key_figure_name in self.keyfigures:
                    _dict[
                        LiveBondKeyFigureName(key_figure_name).name
                    ] = convert_to_float_if_float(key_figure_data["value"])
                    _dict["timestamp"] = str(
                        datetime.fromtimestamp(key_figure_data["updated_at"])
                    )

            # return empty if kf is not available live for bond
            for _kf in self.keyfigures:
                if _kf not in [x["keyfigure"].lower() for x in isin_data["values"]]:
                    _dict[LiveBondKeyFigureName(_kf).name] = ""

            self.dict[isin_data["isin"]] = _dict
        return self.dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _df = pd.DataFrame.from_dict(self.dict, orient="index")
        col = _df.pop("timestamp")
        _df.insert(len(_df.columns), col.name, col)
        _df.index.name = "ISIN"
        return _df.reset_index()
