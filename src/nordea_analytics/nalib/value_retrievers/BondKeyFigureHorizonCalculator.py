import copy
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import numpy as np

from nordea_analytics.convention_variable_names import CashflowType
from nordea_analytics.curve_variable_names import (
    CurveName,
)
from nordea_analytics.key_figure_names import (
    HorizonCalculatedBondKeyFigureName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.exceptions import AnalyticsInputError
from nordea_analytics.nalib.util import (
    convert_to_list,
    convert_to_float_if_float,
    convert_to_original_format,
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class BondKeyFigureHorizonCalculator(ValueRetriever):
    """Retrieves and reformat calculated future bond key figure."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        symbols: Union[str, List[str], pd.Series, pd.Index],
        keyfigures: Union[
            str,
            HorizonCalculatedBondKeyFigureName,
            List[str],
            List[HorizonCalculatedBondKeyFigureName],
            List[Union[str, HorizonCalculatedBondKeyFigureName]],
        ],
        calc_date: datetime,
        horizon_date: datetime,
        curves: Optional[
            Union[
                str,
                CurveName,
                List[str],
                List[CurveName],
                List[Union[str, CurveName]],
            ]
        ] = None,
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
        pp_speed: Optional[float] = None,
        prices: Optional[Union[float, List[float]]] = None,
        cashflow_type: Optional[Union[str, CashflowType]] = None,
        fixed_prepayments: Optional[float] = None,
        prepayments: Optional[Union[float, List[float]]] = None,
        reinvest_in_series: Optional[bool] = None,
        reinvestment_rate: Optional[float] = None,
        spread_change_horizon: Optional[float] = None,
        align_to_forward_curve: Optional[bool] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: The client used to retrieve data.
            symbols: ISIN or name of bonds that should be valued.
            keyfigures: Bond key figure that should be valued.
            calc_date: date of calculation
            horizon_date: future date of calculation
            curves: discount curves for calculation
            shift_tenors: Tenors to shift curves expressed as float. For example [0.25, 0.5, 1, 3, 5].
            shift_values: Shift values in basispoints. For example [100, 100, 75, 100, 100].
            pp_speed: Prepayment speed. Default = 1.
            prices: fixed price per bond.
            cashflow_type: Type of cashflow to calculate with.
            fixed_prepayments: Constant prepayments between calc_cate and horizon date.
                Value of 0.01 would mean that prepayments are set to 1%,
                but model prepayments are still used after horizon date.
                If noting entered, then model prepayments used.
            prepayments: Custom prepayments between calc_cate and horizon date.
                Value of 0.01 would mean that prepayments are set to 1%,
                but model prepayments are still used after horizon date.
                If noting entered, then model prepayments used.
            reinvest_in_series: True if you want to reinvest in the series.
                Default value is True
            reinvestment_rate: Rate you want to reinvest if you don't
                want to reinvest in series. Only relevant if
                    reinvest_in_series is False, or horizon date is
                    further out than maturity of the bond.
            spread_change_horizon: Bump the spread between calc date
                and horizon date. Value should be in bps.
            align_to_forward_curve: True if you want the curve used for horizon
                calculations to be the respective forward curve.
                Default is False.

        Raises:
            AnalyticsInputError: Raises exception with incorrect key figure enum
        """
        super(BondKeyFigureHorizonCalculator, self).__init__(client)
        self._client = client

        self.symbols = convert_to_list(symbols)

        self.key_figures_original: List = (
            keyfigures if isinstance(keyfigures, list) else [keyfigures]
        )

        _keyfigures: List = []
        for keyfigure in self.key_figures_original:
            if isinstance(keyfigure, HorizonCalculatedBondKeyFigureName):
                _keyfigures.append(
                    convert_to_variable_string(
                        keyfigure, HorizonCalculatedBondKeyFigureName
                    )
                )
            elif isinstance(keyfigure, str):
                _keyfigures.append(keyfigure.lower())
            else:
                raise AnalyticsInputError(
                    f"'{type(keyfigure).__name__}' enum is not supported, use '{HorizonCalculatedBondKeyFigureName.__name__}' or '{str.__name__}' instead"
                )

        self.keyfigures = _keyfigures

        self.calc_date = calc_date
        self.horizon_date = horizon_date
        self.curves_original: Union[List, None] = (
            curves
            if isinstance(curves, list)
            else (
                [curves]
                if isinstance(curves, str) or isinstance(curves, CurveName)
                else None
            )
        )

        _curves: Union[List[str], None]
        if isinstance(curves, list):
            _curves = [
                (
                    convert_to_variable_string(curve, CurveName)
                    if isinstance(curve, CurveName)
                    else str(curve)
                )
                for curve in curves
            ]
        elif curves is not None:
            # mypy doesn't know that curves in this line is never a list
            _curves = [convert_to_variable_string(curves, CurveName)]  # type: ignore
        else:
            _curves = None

        self.curves = _curves
        self.shift_tenors = shift_tenors
        self.shift_values = shift_values
        self.pp_speed = pp_speed

        _prices: Union[List[float], None]
        if isinstance(prices, list):
            _prices = prices
        elif prices is not None:
            _prices = [prices]
        else:
            _prices = None

        self.prices = _prices
        self.cashflow_type = (
            convert_to_variable_string(cashflow_type, CashflowType)
            if cashflow_type is not None
            else None
        )
        self.fixed_prepayments = fixed_prepayments
        if isinstance(prepayments, list):
            self.prepayments: Union[float, list[float], None] = prepayments
        elif isinstance(prepayments, float):
            self.prepayments = [prepayments]
        else:
            self.prepayments = None

        self.reinvest_in_series = reinvest_in_series
        self.reinvestment_rate = reinvestment_rate
        self.spread_change_horizon = spread_change_horizon
        self.align_to_forward_curve = align_to_forward_curve

        # Keyfigures that are always returned
        self.fixed_keyfigures = [
            "price",
            "price_at_horizon",
            "return_interest",
            "return_interest_amount",
            "return_principal",
            "return_principal_amount",
            "prepayments",
        ]

        self._data = self.calculate_horizon_bond_key_figure()

    def calculate_horizon_bond_key_figure(self) -> List:
        """Retrieves response with calculated key figures for horizon bond key figure calculation.

        Returns:
            A dictionary containing the calculated key figures, with symbols as keys and responses as values.
        """
        json_response = self.retrieve_response()
        return json_response

    def retrieve_response(self) -> List:
        """Retrieves response after posting the request to the API.

        Returns:
            A dictionary containing the response for each symbol in the request, with symbols as keys and responses as values.
        """
        json_response = self._client.request_calculation(
            {"horizon": self.request}, self.url_suffix
        )
        return json_response

    @property
    def url_suffix(self) -> str:
        """URL suffix for horizon bond key figure calculation.

        Returns:
            The URL suffix for the horizon bond key figure calculation method.
        """
        return config["url_suffix"]["calculate_horizon"]

    @property
    def request(self) -> List[Dict]:
        """Property that generates the post request dictionary for calculating bond key figures.

        Returns:
            A list of dictionaries, each containing the request parameters for a specific bond symbol.
        """
        request_dict = []
        keyfigures = copy.deepcopy(self.keyfigures)
        for kf in self.fixed_keyfigures:
            if kf in self.keyfigures:
                keyfigures.remove(kf)

        if not keyfigures:
            # There has to be at least one key figure in request,
            # but it will not be returned in the final results
            keyfigures = ["yield"]  # type:ignore

        multipleScenarios: bool = (
            self.shift_tenors is not None
            and isinstance(self.shift_tenors, list)
            and any(isinstance(el, list) for el in self.shift_tenors)
        )
        # Single scenario of multiple tenors should be treated as single scenario, solved with list of list
        shift_t: Union[
            List[float],
            List[int],
            List[None],
            List[Union[float, int]],
            List[List[Union[float, int]]],
        ] = (
            self.shift_tenors if multipleScenarios else [self.shift_tenors]  # type: ignore
        )
        shift_v: Union[
            List[float],
            List[int],
            List[None],
            List[Union[float, int]],
            List[List[Union[float, int]]],
        ] = (
            self.shift_values if multipleScenarios else [self.shift_values]  # type: ignore
        )

        for x in range(len(self.symbols)):
            for s in range(len(shift_t)):
                initial_request = {
                    "symbol": self.symbols[x],
                    "date": self.calc_date.strftime("%Y-%m-%d"),
                    "horizon_date": self.horizon_date.strftime("%Y-%m-%d"),
                    "keyfigures": keyfigures,
                    "curves": self.curves,
                    "shift_tenors": shift_t[s],  # type: ignore
                    "shift_values": shift_v[s],  # type: ignore
                    "pp_speed": self.pp_speed,
                    "price": (
                        self.prices[x] if self.prices and x < len(self.prices) else None
                    ),
                    "cashflow_type": self.cashflow_type,
                    "fixed_prepayments": self.fixed_prepayments,
                    "prepayments": self.prepayments,
                    "reinvest_in_series": self.reinvest_in_series,
                    "reinvestment_rate": self.reinvestment_rate,
                    "spread_change_horizon": self.spread_change_horizon,
                    "align_to_forward_curve": self.align_to_forward_curve,
                }
                request = {
                    key: initial_request[key]
                    for key in initial_request.keys()
                    if initial_request[key] is not None
                }
                request_dict.append(request)
        return request_dict

    def to_dict(self) -> Dict:
        """Convert the json response to a dictionary.

        Returns:
            A dictionary containing the bond data, with bond symbols as keys and bond information as values.
        """
        _dict: Dict[Any, Any] = {}
        for i in range(len(self._data)):
            bond_data = self._data[i]
            _dict_bond = self.to_dict_bond(bond_data)

            if "symbol" not in bond_data:  # in case of error from API
                continue

            # When more than one scenario is defined, there are multiple results per symbol
            if any(el == bond_data["symbol"] for el in _dict.keys()) and isinstance(
                _dict[bond_data["symbol"]], list
            ):
                _dict[bond_data["symbol"]].append(_dict_bond)
            else:
                _dict[bond_data["symbol"]] = [_dict_bond]

        return _dict

    def to_dict_bond(self, bond_data: Dict) -> Dict:
        """Convert bond_data to a dictionary with curve data.

        Args:
            bond_data (Dict): Bond data in JSON format.

        Returns:
            Dictionary with curve data extracted from bond_data.
        """
        _dict_bond: Dict[Any, Any] = {}
        for key_figure in bond_data:
            if (
                "price" != key_figure
                and "prepayments" != key_figure
                and key_figure in self.keyfigures
            ):
                data = (
                    bond_data[key_figure]
                    if key_figure in self.fixed_keyfigures
                    else bond_data[key_figure]["values"]
                )
                for curve_data in data:
                    _data_dict: Dict[Any, Any] = {}

                    formatted_result: Union[str, float] = np.nan
                    if "value" in curve_data:
                        formatted_result = convert_to_float_if_float(
                            curve_data["value"]
                        )
                    _data_dict[
                        convert_to_original_format(
                            key_figure, self.key_figures_original
                        )
                    ] = formatted_result

                    curve_key = str()
                    if (
                        self.curves_original is None
                        and curve_data["key"] in CurveName._member_map_
                    ):
                        curve_key = CurveName(curve_data["key"].upper()).name
                    elif self.curves_original is not None:
                        curve_key = convert_to_original_format(
                            curve_data["key"], self.curves_original
                        )
                    else:
                        curve_key = curve_data["key"]

                    if curve_key in _dict_bond.keys():
                        _dict_bond[curve_key].update(_data_dict)
                    else:
                        _dict_bond[curve_key] = _data_dict

        # This would be the case if only Price would be selected as key figure
        # If not, price has no curve to be inserted into
        if _dict_bond == {}:
            _dict_bond["No curve found"] = {}

        if "price" in bond_data and "price" in self.keyfigures:
            for curve in _dict_bond:
                _dict_bond[curve][
                    convert_to_original_format("price", self.key_figures_original)
                ] = bond_data["price"]

        if "prepayments" in self.keyfigures and "prepayments" in bond_data:
            for curve in _dict_bond:
                _dict_bond[curve][
                    convert_to_original_format("prepayments", self.key_figures_original)
                ] = {
                    convert_to_float_if_float(pp["key"]): convert_to_float_if_float(
                        pp["value"]
                    )
                    for pp in bond_data["prepayments"]["values"]
                }

        # Add scenario to result dictionary so users can distinguish between calculation results
        if any(el.lower() == "shift_tenors" for el in bond_data.keys()):
            for curve in _dict_bond:
                _dict_bond[curve]["shift_tenors"] = bond_data["shift_tenors"]
                _dict_bond[curve]["shift_values"] = bond_data["shift_values"]

        return _dict_bond

    def to_df(self) -> pd.DataFrame:
        """Convert bond data to a pandas DataFrame.

        Returns:
            Pandas DataFrame with bond data.
        """
        bond_data_dict = self.to_dict()
        df = pd.DataFrame()

        for symbol in bond_data_dict:
            # Convert the data for the symbol to a DataFrame and transpose it
            for scenarioResult in bond_data_dict[symbol]:
                symbol_df = pd.DataFrame.from_dict(scenarioResult).transpose()
                # Reset the index and rename the columns to "Curve"
                symbol_df = symbol_df.reset_index().rename(columns={"index": "Curve"})
                symbol_df.index = [symbol] * len(symbol_df)

                # Concatenate the symbol DataFrame to the main DataFrame along the rows
                df = pd.concat([df, symbol_df], axis=0)
        return df
