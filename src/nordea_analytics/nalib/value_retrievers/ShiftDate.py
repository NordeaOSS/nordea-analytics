from datetime import datetime
import typing
from typing import Dict, Union

import pandas as pd

from nordea_analytics.convention_variable_names import (
    DateRollConvention,
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


class ShiftDate(ValueRetriever):
    """Shifts a datetime by a given number of days."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        date: datetime,
        days: int = None,
        months: int = None,
        years: int = None,
        exchange: Union[str, Exchange] = None,
        date_roll_convention: Union[str, DateRollConvention] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            date: The date that will be shifted.
            days: The number of days to shift 'date' with.
                Negative values move date back in time.
            months: The number of months to shift 'date' with.
                Negative values move date back in time.
            years: The number of years to shift 'date' with.
                Negative values move date back in time.
            exchange: The exchange's holiday calendar will be used.
            date_roll_convention: The convention to use for rolling
                when a holiday is encountered.
        """
        super(ShiftDate, self).__init__(client)
        self._client = client
        self.date = date
        self.days = days
        self.months = months
        self.years = years
        self.exchange = (
            convert_to_variable_string(exchange, Exchange)
            if type(exchange) == Exchange
            else exchange
        )
        self.date_roll_convention = (
            convert_to_variable_string(date_roll_convention, DateRollConvention)
            if type(date_roll_convention) == DateRollConvention
            else date_roll_convention
        )
        self._data = self.shift_date()

    def shift_date(self) -> Dict:
        """Retrieves response with shifted date."""
        json_response = self.get_response(self.request)

        return json_response[config["results"]["shift_date"]]

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method."""
        return config["url_suffix"]["shift_date"]

    @property
    def request(self) -> dict:
        """Request shifted date."""
        date = self.date.strftime("%Y-%m-%d")
        days = self.days
        months = (self.months,)
        years = (self.years,)
        exchange = self.exchange
        date_roll_convention = self.date_roll_convention

        request_dict = {
            "date": date,
            "days": days,
            "months": months,
            "years": years,
            "exchange": exchange,
            "date-roll-convention": date_roll_convention,
        }

        return request_dict

    def to_datetime(self) -> datetime:
        """Reformat the json response to a datetime."""
        shifted_date_string = typing.cast(str, self._data["date"])

        shifted_date = datetime.strptime(shifted_date_string, "%Y-%m-%d")
        return shifted_date

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        pass

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        pass
