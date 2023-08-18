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
    """Calculate the time between two dates in terms of years.

    Inherits from ValueRetriever class.
    """

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        from_date: datetime,
        to_date: datetime,
        time_convention: Union[str, TimeConvention],
    ) -> None:
        """Initialization of class.

        Args:
            client: The client used to retrieve data.
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
            if isinstance(time_convention, TimeConvention)
            else time_convention
        )
        self._data = self.year_fraction()

    def year_fraction(self) -> Dict:
        """Retrieve response with year fraction.

        Returns:
            JSON response with year fraction.
        """
        json_response = self.get_response(self.request)

        return json_response[config["results"]["year_fraction"]]

    @property
    def url_suffix(self) -> str:
        """URL suffix for the year_fraction method.

        Returns:
            URL suffix for year_fraction.
        """
        return config["url_suffix"]["year_fraction"]

    @property
    def request(self) -> dict:
        """Generate request for year fraction.

        Returns:
            Request dictionary for year fraction.
        """
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
        """Reformat the JSON response to a float.

        Returns:
            Year fraction as a float.
        """
        year_fraction = self._data["year_fraction"]

        return year_fraction

    def to_dict(self) -> Dict:
        """Reformat the JSON response to a dictionary.

        Returns:
            Year fraction as a dictionary.
        """
        pass

    def to_df(self) -> pd.DataFrame:
        """Reformat the JSON response to a pandas DataFrame.

        Returns:
            Year fraction as a DataFrame.
        """
        pass
