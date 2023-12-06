from datetime import datetime
import typing
from typing import Optional, Dict, Union

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
        days: Optional[int] = None,
        months: Optional[int] = None,
        years: Optional[int] = None,
        exchange: Optional[Union[str, Exchange]] = None,
        date_roll_convention: Optional[Union[str, DateRollConvention]] = None,
    ) -> None:
        """Initialization of class.

        Args:
            client: The client used to retrieve data.
            date: The date that will be shifted.
            days: The number of days to shift 'date' with. Negative values move date back in time.
            months: The number of months to shift 'date' with. Negative values move date back in time.
            years: The number of years to shift 'date' with. Negative values move date back in time.
            exchange: The exchange's holiday calendar will be used. If an Exchange object is provided,
                it will be converted to a string.
            date_roll_convention: The convention to use for rolling when a holiday is encountered.
                If a DateRollConvention object is provided, it will be converted to a string.
        """
        super(ShiftDate, self).__init__(client)
        self._client = client
        self.date = date
        self.days = days
        self.months = months
        self.years = years
        self.exchange = (
            convert_to_variable_string(exchange, Exchange)
            if isinstance(exchange, Exchange)
            else exchange
        )
        self.date_roll_convention = (
            convert_to_variable_string(date_roll_convention, DateRollConvention)
            if isinstance(date_roll_convention, DateRollConvention)
            else date_roll_convention
        )
        self._data = self.shift_date()

    def shift_date(self) -> Dict:
        """Shifts the date by the specified number of days, months, and years.

        Returns:
            A dictionary containing the shifted date.
        """
        json_response = self.get_response(self.request)

        return json_response[config["results"]["shift_date"]]

    @property
    def url_suffix(self) -> str:
        """Get the URL suffix for the shift date API method.

        Returns:
            The URL suffix for the shift date API method.
        """
        return config["url_suffix"]["shift_date"]

    @property
    def request(self) -> dict:
        """Generate the request payload for the shift date API method.

        Returns:
            The request payload as a dictionary.
        """
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
        """Convert the JSON response to a datetime object.

        Returns:
            The converted datetime object.
        """
        shifted_date_string = typing.cast(str, self._data["date"])

        shifted_date = datetime.strptime(shifted_date_string, "%Y-%m-%d")
        return shifted_date

    def to_dict(self) -> dict:
        """Convert the JSON response to a dictionary.

        Returns:
            The converted dictionary.
        """
        pass

    def to_df(self) -> pd.DataFrame:
        """Convert the JSON response to a pandas DataFrame.

        Returns:
            The converted pandas DataFrame.
        """
        pass
