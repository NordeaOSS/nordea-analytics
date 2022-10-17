import copy
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Union

import pandas as pd

from nordea_analytics.convention_variable_names import CashflowType
from nordea_analytics.curve_variable_names import (
    CurveName,
)
from nordea_analytics.key_figure_names import (
    CalculatedBondKeyFigureName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    check_json_response,
    convert_to_float_if_float,
    convert_to_variable_string,
    float_to_tenor_string,
    get_config,
)
from nordea_analytics.nalib.util import get_keyfigure_key
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class BondKeyFigureCalculator(ValueRetriever):
    """Retrieves and reformat calculated bond key figure."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        symbols: Union[str, List[str]],
        keyfigures: Union[
            str,
            CalculatedBondKeyFigureName,
            List[str],
            List[CalculatedBondKeyFigureName],
            List[Union[str, CalculatedBondKeyFigureName]],
        ],
        calc_date: datetime,
        curves: Optional[
            Union[
                str,
                CurveName,
                List[str],
                List[CurveName],
                List[Union[str, CurveName]],
            ]
        ] = None,
        rates_shifts: Optional[Union[List[str], str]] = None,
        pp_speed: Optional[float] = None,
        price: Optional[float] = None,
        spread: Optional[float] = None,
        spread_curve: Optional[Union[str, CurveName]] = None,
        yield_input: Optional[float] = None,
        asw_fix_frequency: Optional[str] = None,
        ladder_definition: Optional[List[str]] = None,
        cashflow_type: Optional[Union[str, CashflowType]] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            symbols: ISIN or name of bonds that should be valued.
            keyfigures: Bond key figure that should be valued.
            calc_date: date of calculation.
            curves: discount curves for calculation.
            rates_shifts: shifts in curves("tenor shift in bbp"
                like "0Y 5" or "30Y -5").
            pp_speed: Prepayment speed. Default = 1.
            price: fixed price for bond.
            spread: fixed spread for bond. Mandatory to give
                spread_curve also as an input.
            spread_curve: spread curve to calculate the
                key figures when a fixed spread is given.
            yield_input: fixed yield for bond.
            asw_fix_frequency: Fixing frequency of swap in ASW calculation.
                Mandatory input in all ASW calculations.
            ladder_definition: What tenors should be included in BPV ladder calculation.
            cashflow_type: Type of cashflow to calculate with.
        """
        super(BondKeyFigureCalculator, self).__init__(client)
        self._client = client
        _keyfigures: List = keyfigures if isinstance(keyfigures, list) else [keyfigures]
        self.keyfigures = [
            convert_to_variable_string(keyfigure, CalculatedBondKeyFigureName)
            if isinstance(keyfigure, CalculatedBondKeyFigureName)
            else keyfigure.lower()
            for keyfigure in _keyfigures
        ]
        self.symbols = symbols if isinstance(symbols, list) else [symbols]
        self.calc_date = calc_date
        self.key_figures_original: Union[
            List[str],
            List[CalculatedBondKeyFigureName],
            List[Union[str, CalculatedBondKeyFigureName]],
        ] = (
            keyfigures if isinstance(keyfigures, list) else [keyfigures]
        )
        self.curves_original: Union[
            List[str], List[CurveName], List[Union[str, CurveName]], None
        ] = (
            curves
            if isinstance(curves, list)
            else [curves]
            if isinstance(curves, str)
            else None
        )

        _curves: Union[List[str], None]
        if isinstance(curves, list):
            _curves = [
                convert_to_variable_string(curve, CurveName)
                if isinstance(curve, CurveName)
                else curve
                for curve in curves
            ]
        elif curves is not None:
            # mypy doesn't know that curves in this line is never a list
            _curves = [convert_to_variable_string(curves, CurveName)]  # type: ignore
        else:
            _curves = None

        self.curves = _curves
        self.rates_shifts = rates_shifts
        self.pp_speed = pp_speed
        self.price = price
        self.spread = spread
        _spread_curve = (
            convert_to_variable_string(spread_curve, CurveName)
            if spread_curve
            else None
        )
        self.spread_curve = _spread_curve
        self.yield_input = yield_input
        self.asw_fix_frequency = asw_fix_frequency
        self.ladder_definition = ladder_definition
        self.cashflow_type = (
            convert_to_variable_string(cashflow_type, CashflowType)
            if cashflow_type is not None
            else None
        )
        self._data = self.calculate_bond_key_figure()

    def calculate_bond_key_figure(self) -> Mapping:
        """Retrieves response with calculated key figures."""
        json_response = self.get_post_get_response()

        check_json_response(json_response)
        return json_response

    def get_post_get_response(self) -> Dict:
        """Retrieves response after posting the request."""
        json_response: Dict = {}
        for request_dict in self.request:
            _json_response = self._client.get_post_get_response(
                request_dict, self.url_suffix
            )
            json_response[request_dict["symbol"]] = _json_response

        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix suffix for a given method."""
        return config["url_suffix"]["calculate"]

    @property
    def request(self) -> List[Dict]:
        """Post request dictionary calculate bond key figure."""
        request_dict = []
        keyfigures = copy.deepcopy(self.keyfigures)
        keyfigures.remove("price") if "price" in self.keyfigures else keyfigures
        if keyfigures == []:  # There has to be some key figure in request,
            # but it will not be returned in final results
            keyfigures = "bpv"  # type:ignore
        for symbol in self.symbols:
            initial_request = {
                "symbol": symbol,
                "date": self.calc_date.strftime("%Y-%m-%d"),
                "keyfigures": keyfigures,
                "curves": self.curves,
                "rates_shift": self.rates_shifts,
                "pp_speed": self.pp_speed,
                "price": self.price,
                "spread": self.spread,
                "spread_curve": self.spread_curve,
                "yield": self.yield_input,
                "asw_fix_frequency": self.asw_fix_frequency,
                "ladder_definition": self.ladder_definition,
                "cashflow_type": self.cashflow_type,
            }
            request = {
                key: initial_request[key]
                for key in initial_request.keys()
                if initial_request[key] is not None
            }
            request_dict.append(request)

        return request_dict

    def get_curve_key(self, curve_name: str) -> str:
        """Get curve key for dict."""
        if (
            self.curves_original is not None and curve_name in self.curves_original
        ):  # True when curve is input as string
            curve_key = curve_name
        else:
            try:
                curve_key = CurveName(curve_name).name
            except Exception:
                curve_key = curve_name

        return curve_key

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict: Dict[Any, Any] = {}
        for symbol in self._data:
            bond_data = self._data[symbol]
            _dict_bond = self.to_dict_bond(bond_data)
            _dict[symbol] = _dict_bond
        return _dict

    def to_dict_bond(self, bond_data: Dict) -> Dict:
        """to_dict function too complicated."""
        _dict_bond: Dict[Any, Any] = {}
        for key_figure in bond_data:
            if key_figure != "price" and key_figure in self.keyfigures:
                for curve_data in bond_data[key_figure]["values"]:
                    _data_dict: Dict[Any, Any] = {}
                    if key_figure == "bpvladder":
                        ladder_dict = {
                            float_to_tenor_string(
                                ladder["key"]
                            ): convert_to_float_if_float(ladder["value"])
                            for ladder in curve_data["ladder"]
                        }
                        _data_dict[
                            get_keyfigure_key(
                                key_figure,
                                self.key_figures_original,
                                CalculatedBondKeyFigureName.__name__,
                            )
                        ] = ladder_dict
                    elif key_figure == "expectedcashflow":
                        cashflow_dict = {
                            datetime.strptime(
                                cashflow["payment_date"], "%Y-%m-%d"
                            ).date(): {
                                "interest": cashflow["interest"],
                                "principal": cashflow["principal"],
                            }
                            for cashflow in curve_data["cashflows"]
                        }
                        _data_dict[
                            get_keyfigure_key(
                                key_figure,
                                self.key_figures_original,
                                CalculatedBondKeyFigureName.__name__,
                            )
                        ] = cashflow_dict
                    else:
                        _data_dict[
                            get_keyfigure_key(
                                key_figure,
                                self.key_figures_original,
                                CalculatedBondKeyFigureName.__name__,
                            )
                        ] = convert_to_float_if_float(
                            curve_data["value"]
                        )  # type:ignore

                    curve_key = self.get_curve_key(curve_data["key"])
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
                    get_keyfigure_key(
                        "price",
                        self.key_figures_original,
                        CalculatedBondKeyFigureName.__name__,
                    )
                ] = bond_data["price"]

        return _dict_bond

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()
        df = pd.DataFrame()
        for symbol in _dict:
            _df = pd.DataFrame.from_dict(_dict[symbol]).transpose()
            _df = _df.reset_index().rename(columns={"index": "Curve"})
            _df.index = [symbol] * len(_df)
            df = pd.concat([df, _df], axis=0)
        return df
