from datetime import datetime
import typing
from typing import Dict, List, Union

import pandas as pd

from nordea_analytics.convention_variable_names import (
    DayCountConvention,
    Exchange,
)
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class DateSequence(ValueRetriever):
    """Shifts a datetime by a given number of days."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        from_date: datetime,
        to_date: datetime,
        exchange: Union[str, Exchange] = None,
        day_count_convention: Union[str, DayCountConvention] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            from_date: The start date of the date sequence.
            to_date: The end date of the date sequence.
            exchange: The exchange's holiday calendar will be used.
            day_count_convention: The convention to use for counting days.
        """
        super(DateSequence, self).__init__(client)
        self._client = client
        self.from_date = from_date
        self.to_date = to_date
        self.exchange = (
            convert_to_variable_string(exchange, Exchange)
            if type(exchange) == Exchange
            else exchange
        )
        self.day_count_convention = (
            convert_to_variable_string(day_count_convention, DayCountConvention)
            if type(day_count_convention) == DayCountConvention
            else day_count_convention
        )

        self._data = self.date_sequence()

    def date_sequence(self) -> Dict:
        """Retrieves response with date sequence."""
        json_response = self.get_response(self.request)

        return json_response[config["results"]["date_sequence"]]

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method."""
        return config["url_suffix"]["date_sequence"]

    @property
    def request(self) -> dict:
        """Request shifted date."""
        from_date = self.from_date.strftime("%Y-%m-%d")
        to_date = self.to_date.strftime("%Y-%m-%d")
        exchange = self.exchange
        day_count_convention = self.day_count_convention

        request_dict = {
            "from": from_date,
            "to": to_date,
            "exchange": exchange,
            "day-count-convention": day_count_convention,
        }

        return request_dict

    def to_list_of_datetime(self) -> List:
        """Reformat the json response to a list of datetimes."""
        date_sequence_strings = typing.cast(List, self._data["dates"])

        date_sequence = [
            datetime.strptime(date, "%Y-%m-%d").date() for date in date_sequence_strings
        ]
        return date_sequence

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        pass

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        pass
