from datetime import datetime
import math
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd

from nordea_analytics.instrument_variable_names import BenchmarkName, BondIndexName
from nordea_analytics.key_figure_names import (
    BondKeyFigureName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    convert_to_list,
    convert_to_float_if_float,
    convert_to_original_format,
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class BondKeyFigures(ValueRetriever):
    """Retrieves and reformats given bond key figures for given bonds and calculation date.

    Inherits from ValueRetriever class.
    """

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        symbols: Union[
            str,
            BondIndexName,
            BenchmarkName,
            List[str],
            List[BondIndexName],
            List[BenchmarkName],
            List[Union[str, BondIndexName, BenchmarkName]],
            pd.Series,
            pd.Index,
        ],
        keyfigures: Union[
            str,
            BondKeyFigureName,
            List[str],
            List[BondKeyFigureName],
            List[Union[str, BondKeyFigureName]],
        ],
        calc_date: datetime,
    ) -> None:
        """Initialize the BondKeyFigures class.

        Args:
            client: The client used to retrieve data.
            symbols: ISIN or name of bonds for requests.
                List of bond symbols or a single bond symbol.
            keyfigures: Bond key figure names for request.
                Can be a single key figure name or a list of key figure names.
                Alternatively, can be a single BondKeyFigureName or a list of BondKeyFigureName enums,
                or a list of strings or BondKeyFigureName enums.
            calc_date: Calculation date for request.
        """
        super(BondKeyFigures, self).__init__(client)

        self.symbols = convert_to_list(symbols)

        # Convert keyfigures to a list of strings
        self.keyfigures_original: List = (
            keyfigures if isinstance(keyfigures, list) else [keyfigures]
        )

        # Convert keyfigures to variable string format if it's a BondKeyFigureName enum
        self.keyfigures = [
            (
                convert_to_variable_string(keyfigure, BondKeyFigureName)
                if isinstance(keyfigure, BondKeyFigureName)
                else keyfigure
            )
            for keyfigure in self.keyfigures_original
        ]

        self.calc_date = calc_date
        self._data = self.get_bond_key_figures()

    def get_bond_key_figures(self) -> List:
        """Calls the client and retrieves response with key figures from the service.

        Returns:
            A list of key figures retrieved from the service.
        """
        json_response: List[Any] = []
        for request_dict in self.request:
            _json_response = self.get_response(request_dict)
            json_map = _json_response[config["results"]["bond_key_figures"]]
            json_response = list(json_map) + json_response

        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method.

        Returns:
            The URL suffix for the method.
        """
        return config["url_suffix"]["bond_key_figures"]

    @property
    def request(self) -> List[Dict]:
        """Request list of dictionaries for a given set of symbols, key figures and calc date.

        Returns:
            A list of dictionaries containing request parameters for each batch of symbols.
        """
        if len(self.symbols) > config["max_bonds"]:
            # Split symbols into smaller lists if it exceeds the maximum number of bonds
            split_symbols = np.array_split(
                self.symbols, math.ceil(len(self.symbols) / config["max_bonds"])
            )
            request_dict = [
                {
                    "symbols": list(symbols),
                    "keyfigures": self.keyfigures,
                    "date": self.calc_date.strftime("%Y-%m-%d"),
                }
                for symbols in split_symbols
            ]
        else:
            request_dict = [
                {
                    "symbols": self.symbols,
                    "keyFigures": self.keyfigures,
                    "date": self.calc_date.strftime("%Y-%m-%d"),
                }
            ]

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary.

        Returns:
            A dictionary containing bond symbols as keys and their respective key figures as values.
        """
        _dict = {}
        for bond_data in self._data:
            _bond_dict = {}
            for key_figure_data in bond_data["values"]:
                key_figure_name = convert_to_original_format(
                    key_figure_data["keyfigure"], self.keyfigures_original
                )
                _bond_dict[key_figure_name] = convert_to_float_if_float(
                    key_figure_data["value"]
                )

            _dict[bond_data["symbol"]] = _bond_dict

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame.

        Returns:
            A pandas DataFrame containing bond symbols, key figures, and their values.
        """
        return pd.DataFrame.from_dict(self.to_dict(), orient="index")
