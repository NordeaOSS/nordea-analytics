import copy
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from nordea_analytics.convention_variable_names import (
    SwapDayCountConvention,
    DateRollConvention,
    SwapLegType,
    SwapFixingFrequency,
)
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

config = get_config()


class SwapKeyFigureCalculator(ValueRetriever):
    """Calculate swap key figures.

    Args:
        currency: Currency code.
        keyfigures: Swap key figures that should be valued.
        calc_date: Date of calculation.
        start_date: Optional. Start date of the swap. If not set, calc_date + settlement days.
        end_date: Optional. End date of the swap. If not set, tenor must be set.
        tenor: Optional. Tenor of the swap, e.g. 10Y. If not set, end_date must be set.
        forward: Optional. Forward starting period of the swap, e.g. 1Y.
        fix_frequency_fixed: Optional. Fixing frequency of fixed leg. Allowed values 1D, 1M, 3M, 6M, 1Y.
        fix_frequency_floating: Optional. Fixing frequency of floating leg. Allowed values 1D, 1M, 3M, 6M, 1Y.
        fixed_rate_paid: Optional. Fixed rate of the paid leg. If not set, par rate is used for fixed leg. Expressed in decimals 0.01 => 1%
        fixed_rate_received: Optional. Fixed rate of the received leg. If not set, par rate is used for fixed leg. Expressed in decimals 0.01 => 1%
        floating_spread_paid: Optional. Floating spread of the paid leg. If not set it is 0. Expressed in decimals 0.01 => 100bps
        floating_spread_received: Optional. Floating spread of the received leg. If not set it is 0. Expressed in decimals 0.01 => 100bps
        pay_fixed: Optional. If true, fixed leg will be paying and floating will be receiving. Else vice versa.
        day_count_convention_fixed: Optional. Day count convention of fixed leg.
        day_count_convention_floating: Optional. Day count convention of floating leg.
        date_roll_convention: Optional. Date roll convention of the swap.
        shift_tenors: Optional. Tenors to shift curves expressed as float. For example [0.25, 0.5, 1, 3, 5].
        shift_values: Optional. Shift values in basispoints. For example [100, 100, 75, 100, 100].
        ladder_definition: Optional. Tenors to include in BPV ladder calculation. For example [0.25, 0.5, 1, 3, 5].
    """

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        currency_paid: str,
        currency_received: str,
        keyfigures: Union[
            str,
            SwapKeyFigureName,
            List[str],
            List[SwapKeyFigureName],
            List[Union[str, SwapKeyFigureName]],
        ],
        type_paid: Union[str, SwapLegType],
        type_received: Union[str, SwapLegType],
        calc_date: datetime,
        tenor: Union[str, datetime],
        start_date: Optional[datetime],
        forward: Optional[str],
        fix_frequency_paid: Optional[Union[str, SwapFixingFrequency]],
        fix_frequency_received: Optional[Union[str, SwapFixingFrequency]],
        fixed_rate_paid: Optional[float] = None,
        fixed_rate_received: Optional[float] = None,
        floating_spread_paid: Optional[float] = None,
        floating_spread_received: Optional[float] = None,
        day_count_convention_paid: Optional[Union[str, SwapDayCountConvention]] = None,
        day_count_convention_received: Optional[
            Union[str, SwapDayCountConvention]
        ] = None,
        date_roll_convention: Optional[Union[str, DateRollConvention]] = None,
        shift_tenors: Optional[
            Union[
                float,
                List[float],
                int,
                List[int],
                List[Union[float, int]],
            ]
        ] = None,
        shift_values: Optional[
            Union[
                float,
                List[float],
                int,
                List[int],
                List[Union[float, int]],
            ]
        ] = None,
        ladder_definition: Optional[Union[float, List[float]]] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: The client used to retrieve data.
            currency_paid: Currency code for paid leg.
            currency_received: Currency code for received leg.
            keyfigures: Swap key figures that should be valued.
            type_paid: Whether paid leg should be fixed or floating.
            type_received: Whether received leg should be fixed or floating.
            calc_date: Date of calculation.
            tenor: Tenor of the swap, e.g. 10Y or a datetime.
            start_date: Optional. Start date of the swap. If not set, calc_date + settlement days.
            forward: Optional. Forward starting period of the swap, e.g. 1Y.
            fix_frequency_paid: Optional. Payment frequency of paid leg. Allowed values 1D, 1M, 3M, 6M, 1Y.
            fix_frequency_received: Optional. Payment frequency of receiving leg. Allowed values 1D, 1M, 3M, 6M, 1Y.
            fixed_rate_paid: Optional. Fixed rate of the paid leg. If not set, par rate is used for fixed leg. Expressed in decimals 0.01 => 1%
            fixed_rate_received: Optional. Fixed rate of the received leg. If not set, par rate is used for fixed leg. Expressed in decimals 0.01 => 1%
            floating_spread_paid: Optional. Floating spread of the paid leg. If not set it is 0. Expressed in decimals 0.01 => 100bps
            floating_spread_received: Optional. Floating spread of the received leg. If not set it is 0. Expressed in decimals 0.01 => 100bps
            day_count_convention_paid: Optional. Day count convention of paid leg.
            day_count_convention_received: Optional. Day count convention of received leg.
            date_roll_convention: Optional. Date roll convention of the swap.
            shift_tenors: Optional. Tenors to shift curves expressed as float. For example [0.25, 0.5, 1, 3, 5].
            shift_values: Optional. Shift values in basispoints. For example [100, 100, 75, 100, 100].
            ladder_definition: Optional. Tenors to include in BPV ladder calculation. For example [0.25, 0.5, 1, 3, 5].

        Raises:
            AnalyticsInputError: Raises exception with incorrect key figure enum
        """
        super(SwapKeyFigureCalculator, self).__init__(client)
        self._client = client

        self.currency_paid = currency_paid
        self.currency_received = currency_received

        self.key_figures_original: List = (
            keyfigures if isinstance(keyfigures, list) else [keyfigures]
        )

        _keyfigures: List = []
        for keyfigure in self.key_figures_original:
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
        self.type_paid = (
            convert_to_variable_string(type_paid, SwapLegType)
            if isinstance(type_paid, SwapLegType)
            else type_paid
        )
        self.type_received = (
            convert_to_variable_string(type_received, SwapLegType)
            if isinstance(type_received, SwapLegType)
            else type_received
        )

        self.calc_date = calc_date
        self.tenor = tenor
        self.start_date = start_date
        self.forward = forward
        self.fix_frequency_paid = (
            convert_to_variable_string(fix_frequency_paid, SwapFixingFrequency)
            if isinstance(fix_frequency_paid, SwapFixingFrequency)
            else fix_frequency_paid
        )
        self.fix_frequency_received = (
            convert_to_variable_string(fix_frequency_received, SwapFixingFrequency)
            if isinstance(fix_frequency_received, SwapFixingFrequency)
            else fix_frequency_received
        )
        self.fixed_rate_paid = fixed_rate_paid
        self.fixed_rate_received = fixed_rate_received
        self.floating_spread_paid = floating_spread_paid
        self.floating_spread_received = floating_spread_received

        self.day_count_convention_paid = (
            convert_to_variable_string(
                day_count_convention_paid, SwapDayCountConvention
            )
            if isinstance(day_count_convention_paid, SwapDayCountConvention)
            else day_count_convention_paid
        )
        self.day_count_convention_received = (
            convert_to_variable_string(
                day_count_convention_received, SwapDayCountConvention
            )
            if isinstance(day_count_convention_received, SwapDayCountConvention)
            else day_count_convention_received
        )
        self.date_roll_convention = (
            convert_to_variable_string(date_roll_convention, DateRollConvention)
            if isinstance(date_roll_convention, DateRollConvention)
            else date_roll_convention
        )
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

    def calculate_swap_key_figure(self) -> Dict:
        """Retrieves response with calculated key figures.

        Returns:
            The calculated key figures as a dictionary.
        """
        json_response = self.get_response(self.request)
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
        return config["url_suffix"]["swap_key_figures"]

    @property
    def request(self) -> Dict:
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
            keyfigures = ["pvonts"]  # type:ignore

        request = {
            "currency_paid": self.currency_paid,
            "currency_received": self.currency_received,
            "keyfigures": keyfigures,
            "type_paid": self.type_paid,
            "type_received": self.type_received,
            "date": self.calc_date.strftime("%Y-%m-%d"),
            "tenor": (
                self.tenor.strftime("%Y-%m-%d")
                if isinstance(self.tenor, datetime)
                else self.tenor
            ),
            "start_date": (
                self.start_date.strftime("%Y-%m-%d")
                if self.start_date is not None
                else None
            ),
            "forward": self.forward,
            "fix_frequency_paid": self.fix_frequency_paid,
            "fix_frequency_received": self.fix_frequency_received,
            "fixed_rate_paid": self.fixed_rate_paid,
            "fixed_rate_received": self.fixed_rate_received,
            "floating_spread_paid": self.floating_spread_paid,
            "floating_spread_received": self.floating_spread_received,
            "day_count_convention_paid": self.day_count_convention_paid,
            "day_count_convention_received": self.day_count_convention_received,
            "date_roll_convention": self.date_roll_convention,
            "shift_tenors": self.shift_tenors,
            "shift_values": self.shift_values,
            "ladder_definition": self.ladder_definition,
        }

        return request

    def to_dict(self) -> Dict[str, Any]:
        """Reformat the JSON response to a dictionary.

        Returns:
            A dictionary containing the reformatted JSON data.
        """
        _dict_swap = self.to_dict_swap(self._data)

        return {"Swap": _dict_swap}

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
                    formatted_result = ladder_dict  # type:ignore
                else:
                    formatted_result = convert_to_float_if_float(
                        key_figure_data
                    )  # type:ignore

                _dict_swap[
                    convert_to_original_format(key_figure, self.key_figures_original)
                ] = formatted_result

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
            symbol_df = pd.DataFrame.from_dict(swap_data_dict).transpose()
            symbol_df.index = [symbol] * len(symbol_df)

            # Concatenate the symbol DataFrame to the main DataFrame along the rows
            df = pd.concat([df, symbol_df], axis=0)

        return df
