from datetime import datetime
from typing import Dict, Union

import pandas as pd

from nordea_analytics import TimeConvention
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class YearFraction(ValueRetriever):
    """Calculate the time between two dates in terms of years."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        from_date: datetime,
        to_date: datetime,
        time_convention: Union[str, TimeConvention],
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            from_date: The start date of the time calculation.
            to_date: The end date of the time calculation.
            time_convention: The convention to use for counting time.
        """
        super(YearFraction, self).__init__(client)
        self._client = client
        self.from_date = from_date
        self.to_date = to_date
        self.time_convention = (
            convert_to_variable_string(time_convention, TimeConvention)
            if type(time_convention) == TimeConvention
            else time_convention
        )
        self._data = self.year_fraction()

    def year_fraction(self) -> Dict:
        """Retrieves response with year fraction."""
        json_response = self.get_response(self.request)

        return json_response[config["results"]["year_fraction"]]

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method."""
        return config["url_suffix"]["year_fraction"]

    @property
    def request(self) -> dict:
        """Request year fraction."""
        from_date = self.from_date.strftime("%Y-%m-%d")
        to_date = self.to_date.strftime("%Y-%m-%d")
        time_convention = self.time_convention

        request_dict = {
            "from": from_date,
            "to": to_date,
            "time-convention": time_convention,
        }

        return request_dict

    def to_float(self) -> float:
        """Reformat the json response to a float."""
        year_fraction = self._data["year_fraction"]

        return year_fraction

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        pass

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        pass
