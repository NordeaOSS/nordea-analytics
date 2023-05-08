from datetime import datetime
from typing import Any, Dict, List, Union

import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.exceptions import CustomWarningCheck
from nordea_analytics.nalib.util import (
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class FXQuotes(ValueRetriever):
    """Retrieves FX quotes."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        symbols: Union[List[str], str],
        calc_date: datetime,
    ) -> None:
        """Initialize the FXQuotes class.

        Args:
            client: The client used to retrieve data.
            symbols: Name of FX instruments for request.
            calc_date: Calculation date for request.
        """
        super(FXQuotes, self).__init__(client)

        self.symbols: List = [symbols] if type(symbols) != list else symbols
        self.calc_date = calc_date
        self._data = self.get_fx_quotes()

    def get_fx_quotes(self) -> List:
        """Call the client and retrieve response with FX quotes from the service.

        Returns:
            List of FX quotes retrieved from the service.
        """
        _json_response = self.get_post_get_response()
        json_response: List[Any] = _json_response[config["results"]["fx_quotes"]]

        return json_response

    def get_post_get_response(self) -> Dict:
        """Retrieve response after posting the request.

        Returns:
            JSON response retrieved from the service.
        """
        json_response: Dict = {}
        try:
            json_response = self._client.get_post_get_response(
                self.request, self.url_suffix
            )
        except Exception as ex:
            CustomWarningCheck.post_response_not_retrieved_warning(
                ex, "One or more symbols "
            )

        return json_response

    @property
    def url_suffix(self) -> str:
        """URL suffix for the FX quotes API.

        Returns:
            URL suffix for the FX quotes API.
        """
        return config["url_suffix"]["fx_quotes"]

    @property
    def request(self) -> Dict:
        """Request dictionary for a given set of symbols and calculation date.

        Returns:
            Request dictionary with symbols and calculation date.
        """
        request_dict = {
            "symbols": self.symbols,
            "date": self.calc_date.strftime("%Y-%m-%d"),
        }

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the JSON response to a dictionary.

        Returns:
            Reformatted dictionary with FX quotes.
        """
        _dict = {}
        for bond_data in self._data:
            _bond_dict = {
                "Bid": bond_data["price"]["bid"],
                "Ask": bond_data["price"]["ask"],
            }

            _dict[bond_data["symbol"]] = _bond_dict

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the JSON response to a pandas DataFrame.

        Returns:
            Reformatted pandas DataFrame with FX quotes.
        """
        return pd.DataFrame.from_dict(self.to_dict(), orient="index")
