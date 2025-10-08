from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from nordea_analytics.key_figure_names import (
    CalculatedRepoBondKeyFigureName,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.exceptions import (
    AnalyticsResponseError,
    AnalyticsInputError,
)
from nordea_analytics.nalib.util import (
    convert_to_list,
    convert_to_original_format,
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class BondRepoCalculator(ValueRetriever):
    """Retrieves and reformat calculated repo bond key figure."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        symbols: Union[str, List[str], pd.Series, pd.Index],
        keyfigures: Union[
            str,
            CalculatedRepoBondKeyFigureName,
            List[str],
            List[CalculatedRepoBondKeyFigureName],
            List[Union[str, CalculatedRepoBondKeyFigureName]],
        ],
        calc_date: datetime,
        forward_date: datetime,
        prices: Optional[Union[float, List[float]]] = None,
        forward_prices: Optional[Union[float, List[float]]] = None,
        repo_rates: Optional[Union[float, List[float]]] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            symbols: ISIN or name of bonds that should be valued.
            keyfigures: Bond key figure that should be valued.
            calc_date: date of calculation.
            forward_date: future date of calculation.
            prices: current price of bond.
            forward_prices: future price of bond.
            repo_rates: Repo rate of bond.

        Raises:
            AnalyticsInputError: Raises exception with incorrect key figure enum
        """
        super(BondRepoCalculator, self).__init__(client)
        self._client = client

        self.symbols = convert_to_list(symbols)

        self.key_figures_original: List = (
            keyfigures if isinstance(keyfigures, list) else [keyfigures]
        )

        _keyfigures: List = []
        for keyfigure in self.key_figures_original:
            if isinstance(keyfigure, CalculatedRepoBondKeyFigureName):
                _keyfigures.append(
                    convert_to_variable_string(
                        keyfigure, CalculatedRepoBondKeyFigureName
                    )
                )
            elif isinstance(keyfigure, str):
                _keyfigures.append(keyfigure.lower())
            else:
                raise AnalyticsInputError(
                    f"'{type(keyfigure).__name__}' enum is not supported, use '{CalculatedRepoBondKeyFigureName.__name__}' or '{str.__name__}' instead"
                )

        self.keyfigures = _keyfigures

        self.calc_date = calc_date
        self.forward_date = forward_date

        _prices: Union[List[float], None]
        if isinstance(prices, list):
            _prices = prices
        elif prices is not None:
            _prices = [prices]
        else:
            _prices = None

        self.prices = _prices

        _fwd_prices: Union[List[float], None]
        if isinstance(forward_prices, list):
            _fwd_prices = forward_prices
        elif forward_prices is not None:
            _fwd_prices = [forward_prices]
        else:
            _fwd_prices = None
        self.forward_prices = _fwd_prices

        _repo_rates: Union[List[float], None]
        if isinstance(repo_rates, list):
            _repo_rates = repo_rates
        elif repo_rates is not None:
            _repo_rates = [repo_rates]
        else:
            _repo_rates = None

        self.repo_rates = _repo_rates

        self._check_inputs()

        self._data = self.calculate_repo_bond_key_figure()

    def calculate_repo_bond_key_figure(self) -> List:
        """Retrieves response with calculated key figures."""
        json_response = self.retrieve_response()

        return json_response

    def retrieve_response(self) -> List:
        """Retrieves response after posting the request."""
        json_response = self._client.request_calculation(
            {"repo": self.request}, self.url_suffix
        )
        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method."""
        return config["url_suffix"]["calculate_repo"]

    @property
    def request(self) -> List[Dict]:
        """Post request dictionary calculate bond key figure."""
        request_dict = []
        if self.prices is None:
            parameter_to_calculate = "price"
        elif self.forward_prices is None:
            parameter_to_calculate = "forwardprice"
        elif self.repo_rates is None:
            parameter_to_calculate = "reporate"
        else:
            parameter_to_calculate = ""

        for x in range(len(self.symbols)):
            initial_request = {
                "symbol": self.symbols[x],
                "date": self.calc_date.strftime("%Y-%m-%d"),
                "forward_date": self.forward_date.strftime("%Y-%m-%d"),
                "parameter_to_calculate": parameter_to_calculate,
                "price": (
                    self.prices[x]
                    if self.prices is not None and x < len(self.prices)
                    else None
                ),
                "forward_price": (
                    self.forward_prices[x]
                    if self.forward_prices is not None and x < len(self.forward_prices)
                    else None
                ),
                "repo_rate": (
                    self.repo_rates[x]
                    if self.repo_rates is not None and x < len(self.repo_rates)
                    else None
                ),
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

        for i in range(len(self._data)):
            repo_data = self._data[i]

            if "symbol" not in repo_data:  # in case of error from API
                continue

            _dict_bond = self.to_dict_bond(repo_data)
            _dict[repo_data["symbol"]] = _dict_bond

        return _dict

    def to_dict_bond(self, bond_data: Dict) -> Dict:
        """to_dict function too complicated."""
        _dict_bond: Dict[Any, Any] = {}
        for key_figure in bond_data:
            if key_figure in self.keyfigures:
                _dict_bond[
                    convert_to_original_format(key_figure, self.key_figures_original)
                ] = bond_data[key_figure]
        return _dict_bond

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        return pd.DataFrame.from_dict(self.to_dict()).transpose()

    def _check_inputs(self) -> None:
        if all([self.prices, self.forward_prices, self.repo_rates]):
            raise AnalyticsResponseError(
                """Inputs "Prices", "Forward prices" and "Repo rates" should not "
                                         "all be given as inputs simultaneously."""
            )
        if (
            sum(
                1
                for x in [self.prices, self.forward_prices, self.repo_rates]
                if x is None
            )
            > 1
        ):
            raise AnalyticsResponseError(
                """At least two of the following inputs has to be "
                                         "given: "Prices", "Forward prices", "Repo rates"."""
            )
        if (
            (self.prices is not None and len(self.symbols) != len(self.prices))
            or (
                self.forward_prices is not None
                and len(self.symbols) != len(self.forward_prices)
            )
            or (
                self.repo_rates is not None
                and len(self.symbols) != len(self.repo_rates)
            )
        ):
            raise AnalyticsResponseError(
                """Inputs "Prices", "Forward Prices" and "Repo Rates" have to "
                                         "have the same length as input "Symbols" """
            )
