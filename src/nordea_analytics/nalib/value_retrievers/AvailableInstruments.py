from typing import Dict

import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class AvailableInstruments(ValueRetriever):
    """Retrieves all available instruments (excluding FX).

    Inherits from ValueRetriever class.
    """

    def __init__(
        self,
        client: DataRetrievalServiceClient,
    ) -> None:
        """Initialize the AvailableInstruments class.

        Args:
            client: The client used to retrieve data.
        """
        super(AvailableInstruments, self).__init__(client)

        self._data = self.get_available_instruments()

    def get_available_instruments(self) -> Dict:
        """Calls the client and retrieves response with available instruments from the service.

        Returns:
            A list of available instruments from the service.
        """
        json_response = self.get_response(self.request)[
            config["results"]["available_instruments"]
        ]["names"]

        return {"available_instruments": json_response}

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method.

        Returns:
            The URL suffix for the method.
        """
        return config["url_suffix"]["available_instruments"]

    @property
    def request(self) -> Dict:
        """Request list of dictionaries for a given set of symbols, key figures and calc date.

        Returns:
            A list of dictionaries containing request parameters for each batch of symbols.
        """
        return {}

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary.

        Returns:
            A dictionary containing bond symbols as keys and their respective key figures as values.
        """
        return self._data

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame.

        Returns:
            A pandas DataFrame containing bond symbols, key figures, and their values.
        """
        df = pd.DataFrame.from_dict(self.to_dict())
        df.columns = ["Instrument"]
        return df
