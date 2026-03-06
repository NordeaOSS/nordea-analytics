import copy
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from nordea_analytics.key_figure_names import (
    SwapKeyFigureName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.exceptions import AnalyticsInputError
from nordea_analytics.nalib.util import (
    convert_to_float_if_float,
    convert_to_original_format,
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever
from nordea_analytics.swap_definition import SwapDefinition  # type: ignore[attr-defined]

config = get_config()


class SwapKeyFigureCalculator(ValueRetriever):
    """Calculate swap key figures.

    Args:
        swaps: Call method build_swaps() and add the resulting dictionary with swap definitions.
        keyfigures: Optional. Swap key figures that should be valued.
        calc_date: Optional. Date of calculation.
        shift_tenors: Optional. Tenors to shift curves expressed as float. For example [0.25, 0.5, 1, 3, 5].
        shift_values: Optional. Shift values in basispoints. For example [100, 100, 75, 100, 100].
        ladder_definition: Optional. Tenors to include in BPV ladder calculation. For example [0.25, 0.5, 1, 3, 5].
    """

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        swaps: Union[SwapDefinition, list[SwapDefinition], dict[str, SwapDefinition]],
        keyfigures: Union[
            str,
            SwapKeyFigureName,
            List[str],
            List[SwapKeyFigureName],
            List[Union[str, SwapKeyFigureName]],
            pd.Series,
            pd.Index,
        ],
        calc_date: datetime,
        shift_tenors: Optional[
            Union[
                float,
                List[float],
                int,
                List[int],
                List[Union[float, int]],
                List[List[Union[float, int]]],
            ]
        ] = None,
        shift_values: Optional[
            Union[
                float,
                List[float],
                int,
                List[int],
                List[Union[float, int]],
                List[List[Union[float, int]]],
            ]
        ] = None,
        ladder_definition: Optional[Union[float, List[float]]] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: The client used to retrieve data.
            swaps: Call method build_swaps() and add the retrieved list of SwapDefinitions or create it manually.
            keyfigures: Swap key figures that should be valued.
            calc_date: Date of calculation.
            shift_tenors: Optional. Tenors to shift curves expressed as float. For example [0.25, 0.5, 1, 3, 5].
            shift_values: Optional. Shift values in basispoints. For example [100, 100, 75, 100, 100].
            ladder_definition: Optional. Tenors to include in BPV ladder calculation. For example [0.25, 0.5, 1, 3, 5].

        Raises:
            AnalyticsInputError: Raises exception with incorrect key figure enum
        """
        super(SwapKeyFigureCalculator, self).__init__(client)
        self._client = client

        if isinstance(swaps, list):
            self.swaps = swaps
        elif isinstance(swaps, dict):
            self.swaps = list(swaps.values())
        elif isinstance(swaps, SwapDefinition):
            self.swaps = [swaps]

        self.calc_date = calc_date

        self.keyfigures_original: List = (
            keyfigures if isinstance(keyfigures, list) else [keyfigures]
        )

        _keyfigures: List = []
        for keyfigure in self.keyfigures_original:
            if isinstance(keyfigure, SwapKeyFigureName):
                _keyfigures.append(
                    convert_to_variable_string(keyfigure, SwapKeyFigureName)
                )
            elif isinstance(keyfigure, str):
                _keyfigures.append(keyfigure.lower())
            else:
                raise AnalyticsInputError(
                    f"'{type(keyfigure).__name__}' enum is not supported, use '{SwapKeyFigureName.__name__}' or '{str.__name__}' instead"
                )
        self.keyfigures = _keyfigures
        self.shift_tenors = shift_tenors
        self.shift_values = shift_values
        self.ladder_definition = ladder_definition

        # Keyfigures that are always returned
        self.fixed_keyfigures = [
            "fixed_rate_paid",
            "fixed_rate_received",
            "floating_spread_paid",
            "floating_spread_received",
        ]

        self._data = self.calculate_swap_key_figure()

    def calculate_swap_key_figure(self) -> List:
        """Retrieves response with calculated key figures.

        Returns:
            The calculated key figures as a dictionary.
        """
        json_response = self._client.request_calculation(
            {"swap_standard": self.request}, self.url_suffix
        )
        return json_response

    def get_response(self, request: Dict) -> Dict:
        """Call the DataRetrievalServiceClient to get a response from the service.

        Args:
            request (Dict): The request dictionary.

        Returns:
            Dict: The response from the service for a given method and request.
        """
        json_response = self._client.post_response_asynchronous(
            request, self.url_suffix
        )
        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method.

        Returns:
            The URL suffix for the bond calculator method.
        """
        return config["url_suffix"]["calculate"]

    @property
    def request(self) -> list[Dict]:
        """Post request dictionary to calculate swap key figures.

        Returns:
            Request dictionary to calculate swap key figures.
        """
        keyfigures = copy.deepcopy(self.keyfigures)
        for kf in self.fixed_keyfigures:
            if kf in self.keyfigures:
                keyfigures.remove(kf)

        if not keyfigures:
            # There has to be at least one key figure in request,
            # but it will not be returned in the final results
            keyfigures = ["pvonts"]  # type: ignore

        multipleScenarios: bool = (
            self.shift_tenors is not None
            and isinstance(self.shift_tenors, list)
            and any(isinstance(el, list) for el in self.shift_tenors)
        )

        shift_tenors: Union[
            List[float],
            List[int],
            List[None],
            List[Union[float, int]],
            List[List[Union[float, int]]],
        ] = (
            self.shift_tenors if multipleScenarios else [self.shift_tenors]  # type: ignore
        )
        shift_values: Union[
            List[float],
            List[int],
            List[None],
            List[Union[float, int]],
            List[List[Union[float, int]]],
        ] = (
            self.shift_values if multipleScenarios else [self.shift_values]  # type: ignore
        )

        request_dict = []
        for swap in self.swaps:
            for s in range(len(shift_tenors)):
                initial_request = {
                    "currency_paid": swap.currency_paid,
                    "currency_received": swap.currency_received,
                    "keyfigures": keyfigures,
                    "type_paid": swap.type_paid,
                    "type_received": swap.type_received,
                    "date": self.calc_date.strftime("%Y-%m-%d"),
                    "tenor": (
                        swap.tenor.strftime("%Y-%m-%d")
                        if isinstance(swap.tenor, datetime)
                        else swap.tenor
                    ),
                    "start": (
                        swap.start.strftime("%Y-%m-%d")
                        if isinstance(swap.start, datetime)
                        else swap.start
                    ),
                    "fix_frequency_paid": swap.fix_frequency_paid,
                    "fix_frequency_received": swap.fix_frequency_received,
                    "fixed_rate_paid": swap.fixed_rate_paid,
                    "fixed_rate_received": swap.fixed_rate_received,
                    "floating_spread_paid": swap.floating_spread_paid,
                    "floating_spread_received": swap.floating_spread_received,
                    "day_count_convention_paid": swap.day_count_convention_paid,
                    "day_count_convention_received": swap.day_count_convention_received,
                    "date_roll_convention": swap.date_roll_convention,
                    "shift_tenors": shift_tenors[s],
                    "shift_values": shift_values[s],
                    "ladder_definition": self.ladder_definition,
                }
                request = {
                    key: initial_request[key]
                    for key in initial_request.keys()
                    if initial_request[key] is not None
                }

                request_dict.append(request)

        return request_dict

    def to_dict(self) -> Dict[str, Any]:
        """Reformat the JSON response to a dictionary.

        Returns:
            A dictionary containing the reformatted JSON data.
        """
        _dict: Dict[Any, Any] = {}
        for i in range(len(self._data)):
            swap_data = self._data[i]
            _dict_swap = self.to_dict_swap(swap_data)

            if "symbol" not in swap_data:  # in case of error from API
                continue

            # When more than one scenario is defined, there are multiple results per symbol
            if any(el == swap_data["symbol"] for el in _dict.keys()) and isinstance(
                _dict[swap_data["symbol"]], list
            ):
                _dict[swap_data["symbol"]].append(_dict_swap)
            else:
                _dict[swap_data["symbol"]] = [_dict_swap]

        return _dict

    def to_dict_swap(self, swap_data: Dict) -> Dict:
        """Reformat the JSON bond data to a dictionary.

        Args:
            swap_data: The JSON data of a bond.

        Returns:
            A dictionary containing the reformatted bond data.
        """
        _dict_swap: Dict[Any, Any] = {}

        for key_figure in swap_data:
            if key_figure in self.keyfigures:
                key_figure_data = swap_data[key_figure]

                if key_figure == "bpvladder":
                    # Convert ladder data to dictionary
                    ladder_dict = {
                        convert_to_float_if_float(
                            ladder["key"]
                        ): convert_to_float_if_float(ladder["value"])
                        for ladder in key_figure_data
                    }
                    formatted_result = ladder_dict  # type: ignore
                else:
                    formatted_result = convert_to_float_if_float(
                        key_figure_data
                    )  # type: ignore

                _dict_swap[
                    convert_to_original_format(key_figure, self.keyfigures_original)
                ] = formatted_result

        if any(el.lower() == "shift_tenors" for el in swap_data.keys()):
            _dict_swap["shift_tenors"] = swap_data["shift_tenors"]
            _dict_swap["shift_values"] = swap_data["shift_values"]

        return _dict_swap

    def to_df(self) -> pd.DataFrame:
        """Reformat the JSON response of bond data to a pandas DataFrame.

        Returns:
            A pandas DataFrame containing the reformatted bond data.
        """
        swap_data_dict = self.to_dict()
        df = pd.DataFrame()

        for symbol in swap_data_dict:
            # Convert the data for the symbol to a DataFrame and transpose it
            for scenarioResult in swap_data_dict[symbol]:
                symbol_df = pd.DataFrame.from_dict(scenarioResult, orient="index").T
                symbol_df.index = [symbol] * len(symbol_df)

                # Concatenate the symbol DataFrame to the main DataFrame along the rows
                df = pd.concat([df, symbol_df], axis=0)

        return df
