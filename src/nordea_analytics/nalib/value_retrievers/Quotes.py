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


class Quotes(ValueRetriever):
    """Retrieves FX quotes.

    This class inherits from ValueRetriever and provides methods to retrieve and reformat FX quote data
    from a DataRetrievalServiceClient instance.
    """

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        symbols: Union[List[str], str],
        calc_date: datetime,
    ) -> None:
        """Initializes the Quotes instance.

        Args:
            client: The client used to retrieve data.
            symbols: Name of instruments for request.
            calc_date: Calculation date for request.
        """
        super(Quotes, self).__init__(client)

        # Convert symbols to list if it's not already a list
        self.symbols: List = [symbols] if type(symbols) != list else symbols
        self.calc_date = calc_date
        self._data = self.get_fx_quotes()

    def get_fx_quotes(self) -> List:
        """Calls the client and retrieves response with FX quote data from the service.

        Returns:
            A list of dictionaries containing FX quote data.
        """
        _json_response = self.get_post_get_response()
        json_response: List[Any] = _json_response[config["results"]["quotes"]]

        return json_response

    def get_post_get_response(self) -> Dict:
        """Retrieves response after posting the request.

        Returns:
            A dictionary containing the response data.
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
        """Returns the URL suffix for FX quotes.

        Returns:
            The URL suffix for FX quotes.
        """
        return config["url_suffix"]["quotes"]

    @property
    def request(self) -> Dict:
        """Returns a request dictionary for a given set of symbols and calculation date.

        Returns:
            A dictionary representing the request data.
        """
        request_dict = {
            "symbols": self.symbols,
            "date": self.calc_date.strftime("%Y-%m-%d"),
        }

        return request_dict

    def to_dict(self) -> Dict:
        """Reformats the JSON response to a dictionary.

        Returns:
            A dictionary containing the reformatted data from the JSON response.
        """
        _dict = {}
        for quote_data in self._data:
            _dict[quote_data["symbol"]] = quote_data["quote"]

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformats the JSON response to a pandas DataFrame.

        Returns:
            A pandas DataFrame containing the reformatted data from the JSON response.
        """
        return pd.DataFrame.from_dict(self.to_dict(), orient="index", columns=["Quote"])
