from datetime import datetime
from typing import Any, Dict, Mapping, Union

import pandas as pd

from nordea_analytics.forecast_names import YieldCountry, YieldHorizon, YieldType
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    convert_to_variable_string,
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class YieldForecast(ValueRetriever):
    """Retrieves and reformat Yield forecast."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        country: Union[str, YieldCountry],
        yield_type: Union[str, YieldType],
        yield_horizon: Union[str, YieldHorizon],
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            country: Country of the yield for which to retrieve forecasts.
            yield_type: Type of yield for which to retrieve forecasts.
            yield_horizon: Horizon for which to retrieve forecasts.
        """
        super(YieldForecast, self).__init__(client)
        self._client = client
        self.country = convert_to_variable_string(country, YieldCountry)
        self.yield_type = convert_to_variable_string(yield_type, YieldType)
        self.yield_horizon = convert_to_variable_string(yield_horizon, YieldHorizon)
        self._data = self.get_yield_forecast()

    def get_yield_forecast(self) -> Mapping:
        """Retrieves response with yield forecast."""
        json_response = self.get_response(self.request)
        json_response = json_response[config["results"]["yield_forecast"]]

        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method."""
        return config["url_suffix"]["yield_forecast"]

    @property
    def request(self) -> Dict:
        """Request dictionary yield forecast."""
        country = self.country
        yield_type = self.yield_type
        yield_horizon = self.yield_horizon

        request_dict = {
            "country": country,
            "yield-type": yield_type,
            "yield-horizon": yield_horizon,
        }

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict: Dict[Any, Any] = {}

        forecast_data = {}

        for yield_type_data in self._data["forecasts"]:
            yield_type_forecast_data = {}
            type = yield_type_data["type"]

            for data in yield_type_data["forecast"]:
                values = {}

                values["Updated_at"] = datetime.strptime(
                    yield_type_data["updated_at"].split("T")[0], "%Y-%m-%d"
                )

                values["Value"] = data["value"]
                yield_type_forecast_data[data["horizon"]] = values

            forecast_data[type] = yield_type_forecast_data

        _dict[self._data["symbol"]] = forecast_data

        return _dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the json response to a pandas DataFrame."""
        _dict = self.to_dict()

        df = pd.DataFrame.from_dict(
            {
                (i, j, k): _dict[i][j][k]
                for i in _dict.keys()
                for j in _dict[i].keys()
                for k in _dict[i][j].keys()
            },
            orient="index",
        )
        df = df.reset_index().rename(
            columns={"level_0": "Symbol", "level_1": "Yield_type", "level_2": "Horizon"}
        )

        return df
