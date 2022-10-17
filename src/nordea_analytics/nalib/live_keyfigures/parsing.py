from datetime import datetime
from typing import Dict, List, Union

import pandas as pd

from nordea_analytics.key_figure_names import LiveBondKeyFigureName
from nordea_analytics.nalib.util import convert_to_float_if_float, get_keyfigure_key


def filter_keyfigures(
    chunk: Dict,
    key_figures: List[str],
    key_figures_original: List[Union[str, LiveBondKeyFigureName]],
) -> Dict:
    """Reformat the json dict to filter only desire keyfigure values.

    Args:
        chunk: Dict which contains original data.
        key_figures: keyfigures to filter.
        key_figures_original: keyfigures passed by user in same format, case sensitive

    Returns:
        Filtered live keyfigures dict.
    """
    result = {}
    for key_figure_data in chunk["values"]:
        key_figure_name = key_figure_data["keyfigure"].lower()
        key_figure_key = get_keyfigure_key(
            key_figure_name, key_figures_original, LiveBondKeyFigureName.__name__
        )
        if key_figure_name in key_figures:
            result[key_figure_key] = convert_to_float_if_float(key_figure_data["value"])
            timestamp = (
                key_figure_data["timestamp"]
                if "timestamp" in key_figure_data
                else key_figure_data["updated_at"]
            )
            result["timestamp"] = str(datetime.fromtimestamp(timestamp))
    # return empty if kf is not available live for bond
    for _kf in key_figures:
        key_figure_key = get_keyfigure_key(
            _kf, key_figures_original, LiveBondKeyFigureName.__name__
        )
        if _kf not in [x["keyfigure"].lower() for x in chunk["values"]]:
            result[LiveBondKeyFigureName(key_figure_key).name] = ""

    d = {chunk["isin"]: result}
    return d


def to_data_frame(live_keyfigures_dict: Dict) -> pd.DataFrame:
    """Reformat the live keyfigures Dict to a Pandas DataFrame.

    Args:
        live_keyfigures_dict: Dict to be converted to Panda DataFrame

    Returns:
        Instance of pd.DataFrame.
    """
    df = pd.DataFrame.from_dict(live_keyfigures_dict, orient="index")
    if df.empty:
        return df
    col = df.pop("timestamp")
    df.insert(len(df.columns), col.name, col)
    df.index.name = "ISIN"
    return df.reset_index()


# Use factory here to avoid strategy pattern for parsing json from different endpoints
def parse_live_keyfigures_json(
    json_payload: Dict,
    keyfigures: List[str],
    keyfigures_original: List[Union[LiveBondKeyFigureName, str]],
) -> Dict:
    """Reformat the json with livekeyfigures to filter only desire keyfigure values.

    Args:
        json_payload: original JSON response from server in form of Dict.
        keyfigures: keyfigures to filter.
        keyfigures_original: original keyfigures as inserted by user, as enum or str

    Returns:
        Filtered dict.
    """
    # "data" is part of request/response REST API and not HTTP streaming
    if "data" in json_payload:
        results: Dict = {}
        for values in json_payload["data"]["keyfigure_values"]:
            results = results | filter_keyfigures(
                values, keyfigures, keyfigures_original
            )
        return results
    else:
        return filter_keyfigures(json_payload, keyfigures, keyfigures_original)
