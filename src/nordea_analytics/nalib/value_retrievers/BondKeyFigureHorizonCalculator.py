import copy
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Union

import pandas as pd

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
from nordea_analytics.nalib.exceptions import CustomWarningCheck
from nordea_analytics.nalib.http.errors import BadRequestError
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
        shift_tenors: Optional[Union[List[float], float]] = None,
        shift_values: Optional[Union[List[float], float]] = None,
        pp_speed: Optional[float] = None,
        prices: Optional[Union[float, List[float]]] = None,
        cashflow_type: Optional[Union[str, CashflowType]] = None,
        fixed_prepayments: Optional[float] = None,
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
            fixed_prepayments: repayments between calc_cate and horizon date.
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
        """
        super(BondKeyFigureHorizonCalculator, self).__init__(client)
        self._client = client

        self.symbols = convert_to_list(symbols)

        self.key_figures_original: List = (
            keyfigures if isinstance(keyfigures, list) else [keyfigures]
        )
        self.keyfigures = [
            (
                convert_to_variable_string(kf, HorizonCalculatedBondKeyFigureName)
                if isinstance(kf, HorizonCalculatedBondKeyFigureName)
                else kf.lower()
            )
            for kf in self.key_figures_original
        ]

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
        self.reinvest_in_series = reinvest_in_series
        self.reinvestment_rate = reinvestment_rate
        self.spread_change_horizon = spread_change_horizon
        self.align_to_forward_curve = align_to_forward_curve
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

    def calculate_horizon_bond_key_figure(self) -> Mapping:
        """Retrieves response with calculated key figures for horizon bond key figure calculation.

        Returns:
            A dictionary containing the calculated key figures, with symbols as keys and responses as values.
        """
        json_response = self.retrieve_response()
        return json_response

    def retrieve_response(self) -> Dict:
        """Retrieves response after posting the request to the API.

        Returns:
            A dictionary containing the response for each symbol in the request, with symbols as keys and responses as values.
        """
        json_response: Dict = {}
        for request_dict in self.request:
            try:
                # Call asynchronous method to get response and store it in json_response dictionary
                _json_response = self._client.get_response_asynchronous(
                    request_dict, self.url_suffix
                )
                json_response[request_dict["symbol"]] = _json_response
            except BadRequestError as bad_request:
                CustomWarningCheck.bad_request_warning(
                    bad_request, request_dict["symbol"]
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

        for x in range(len(self.symbols)):
            initial_request = {
                "symbol": self.symbols[x],
                "date": self.calc_date.strftime("%Y-%m-%d"),
                "horizon_date": self.horizon_date.strftime("%Y-%m-%d"),
                "keyfigures": keyfigures,
                "curves": self.curves,
                "shift_tenors": self.shift_tenors,
                "shift_values": self.shift_values,
                "pp_speed": self.pp_speed,
                "price": (
                    self.prices[x] if self.prices and x < len(self.prices) else None
                ),
                "cashflow_type": self.cashflow_type,
                "fixed_prepayments": self.fixed_prepayments,
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
        for symbol in self._data:
            bond_data = self._data[symbol]
            _dict_bond = self.to_dict_bond(bond_data)
            _dict[symbol] = _dict_bond

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
                    formatted_result = convert_to_float_if_float(curve_data["value"])
                    _data_dict[
                        convert_to_original_format(
                            key_figure, self.key_figures_original
                        )
                    ] = formatted_result
                    curve_key = (
                        CurveName(curve_data["key"].upper()).name
                        if self.curves_original is None
                        else convert_to_original_format(
                            curve_data["key"], self.curves_original  # type:ignore
                        )
                    )
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

        return _dict_bond

    def to_df(self) -> pd.DataFrame:
        """Convert bond data to a pandas DataFrame.

        Returns:
            Pandas DataFrame with bond data.
        """
        _dict = self.to_dict()
        df = pd.DataFrame()
        for symbol in _dict:
            _df = pd.DataFrame.from_dict(_dict[symbol]).transpose()
            _df = _df.reset_index().rename(columns={"index": "Curve"})
            _df.index = [symbol] * len(_df)
            df = pd.concat([df, _df], axis=0)
        return df
