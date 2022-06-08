import copy
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Union

import pandas as pd

from nordea_analytics.curve_variable_names import (
    CurveName,
)
from nordea_analytics.key_figure_names import (
    HorizonCalculatedBondKeyFigureName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    check_json_response,
    check_json_response_error,
    convert_to_float_if_float,
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
        isins: Union[str, List[str]],
        keyfigures: Union[
            str,
            HorizonCalculatedBondKeyFigureName,
            List[str],
            List[HorizonCalculatedBondKeyFigureName],
            List[Union[str, HorizonCalculatedBondKeyFigureName]],
        ],
        calc_date: datetime,
        horizon_date: datetime,
        curves: Optional[Union[List[str], str, CurveName, List[CurveName]]] = None,
        rates_shifts: Optional[Union[List[str], str]] = None,
        pp_speed: Optional[float] = None,
        price: Optional[float] = None,
        fixed_prepayments: Optional[float] = None,
        reinvest_in_series: Optional[bool] = None,
        reinvestment_rate: Optional[float] = None,
        spread_change_horizon: Optional[float] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            isins: ISINs of bond that should be valued.
            keyfigures: Bond key figure that should be valued.
            calc_date: date of calculation
            horizon_date: future date of calculation
            curves: discount curves for calculation
            rates_shifts: shifts in curves("tenor shift in bbp"
                like "0Y 5" or "30Y -5").
            pp_speed: Prepayment speed. Default = 1.
            price: fixed price for ISIN.
            fixed_prepayments: repayments between calc_cate and horizon date.
                Value of 0.01 would mean that prepayments are set to 1%,
                but model prepayments are still used after horizon date.
                If noting entered, then model prepayments used.
            reinvest_in_series:  True if you want to reinvest in the series.
                Default value is True
            reinvestment_rate: Rate you want to reinvest if you don't
                want to reinvest in series. Only relevant if
                    reinvest_in_series is False, or horizon date is
                    further out than maturity of the bond.
            spread_change_horizon:bump the spread between calc date
                and horizon date. Value should be in bps.
        """
        super(BondKeyFigureHorizonCalculator, self).__init__(client)
        self._client = client
        _keyfigures: List = keyfigures if type(keyfigures) == list else [keyfigures]
        self.keyfigures = [
            convert_to_variable_string(keyfigure, HorizonCalculatedBondKeyFigureName)
            for keyfigure in _keyfigures
        ]
        self.isins = [isins] if type(isins) == str else isins
        self.calc_date = calc_date
        self.horizon_date = horizon_date
        _curves: Union[
            str,
            List[str],
            ValueError,
            List[ValueError],
            List[Union[str, ValueError]],
            None,
        ] = None
        if curves is not None:
            _curves = (
                [convert_to_variable_string(curve, CurveName) for curve in curves]
                if type(curves) == list
                # mypy doesn't know that curves in this line is never a list
                else [convert_to_variable_string(curves, CurveName)]  # type: ignore
            )
        self.curves = _curves
        self.rates_shifts = rates_shifts
        self.pp_speed = pp_speed
        self.price = price
        self.fixed_prepayments = fixed_prepayments
        self.reinvest_in_series = reinvest_in_series
        self.reinvestment_rate = reinvestment_rate
        self.spread_change_horizon = spread_change_horizon
        self.fixed_keyfigures = [
            "price",
            "price_at_horizon",
            "return_interest",
            "return_interest_amount",
            "return_principal",
            "return_principal_amount",
        ]

        self._data = self.calculate_horizon_bond_key_figure()

    def calculate_horizon_bond_key_figure(self) -> Mapping:
        """Retrieves response with calculated key figures."""
        json_response = self.get_post_get_response()

        output_found = check_json_response(json_response)
        check_json_response_error(output_found)

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
        return config["url_suffix"]["calculate_horizon"]

    @property
    def request(self) -> List[Dict]:
        """Post request dictionary calculate bond key figure."""
        request_dict = []
        keyfigures = copy.deepcopy(self.keyfigures)
        for kf in self.fixed_keyfigures:
            if kf in self.keyfigures:
                keyfigures.remove(kf)

        if keyfigures == []:  # There has to be some key figure in request,
            # but it will not be returned in final results
            keyfigures = "bpv"  # type:ignore
        for isin in self.isins:
            initial_request = {
                "symbol": isin,
                "date": self.calc_date.strftime("%Y-%m-%d"),
                "horizon_date": self.horizon_date.strftime("%Y-%m-%d"),
                "keyfigures": keyfigures,
                "curves": self.curves,
                "rates_shift": self.rates_shifts,
                "pp_speed": self.pp_speed,
                "price": self.price,
                "fixed_prepayments": self.fixed_prepayments,
                "reinvest_in_series": self.reinvest_in_series,
                "reinvestment_rate": self.reinvestment_rate,
                "spread_change_horizon": self.spread_change_horizon,
            }
            request = {
                key: initial_request[key]
                for key in initial_request.keys()
                if initial_request[key] is not None
            }
            request_dict.append(request)
        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict: Dict[Any, Any] = {}
        for isin in self._data:
            isin_data = self._data[isin]
            _dict_isin: Dict[Any, Any] = {}
            for key_figure in isin_data:
                if "price" not in key_figure and key_figure in self.keyfigures:
                    data = (
                        isin_data[key_figure]
                        if key_figure in self.fixed_keyfigures
                        else isin_data[key_figure]["values"]
                    )
                    for curve_data in data:
                        _data_dict = {}
                        _data_dict[
                            HorizonCalculatedBondKeyFigureName(key_figure).name
                        ] = convert_to_float_if_float(curve_data["value"])
                        if curve_data["key"] in _dict_isin.keys():
                            _dict_isin[curve_data["key"]].update(_data_dict)
                        else:
                            _dict_isin[curve_data["key"]] = _data_dict

            if (
                _dict_isin == {}
            ):  # This would be the case if only Price would be selected as key figure
                for curve_data in isin_data["bpv"]["values"]:
                    _dict_isin[curve_data["key"]] = {}

            if "price" in isin_data and "price" in self.keyfigures:
                for curve in _dict_isin:
                    _dict_isin[curve][
                        HorizonCalculatedBondKeyFigureName("price").name
                    ] = isin_data["price"]

            _dict[isin] = _dict_isin

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()
        df = pd.DataFrame()
        for isin in _dict:
            _df = pd.DataFrame.from_dict(_dict[isin]).transpose()
            _df = _df.reset_index().rename(columns={"index": "Curve"})
            _df.index = [isin] * len(_df)
            df = df.append(_df)
        return df
