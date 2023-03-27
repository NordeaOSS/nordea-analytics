from datetime import datetime
from typing import Any, Dict, Mapping

import pandas as pd

from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import (
    get_config,
)
from nordea_analytics.nalib.value_retriever import ValueRetriever

config = get_config()


class FXForecast(ValueRetriever):
    """Retrieves and reformat FX forecast."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        currency_pair: str,
    ) -> None:
        """Initialization of class.

        Args:
            client: DataRetrievalServiceClient
                or DataRetrievalServiceClientTest for testing.
            currency_pair: Currency cross for which to retrieve forecasts.
        """
        super(FXForecast, self).__init__(client)
        self._client = client
        self.currency_pair = currency_pair
        self._data = self.get_fx_forecast()

    def get_fx_forecast(self) -> Mapping:
        """Retrieves response with FX forecast."""
        json_response = self.get_response(self.request)
        json_response = json_response[config["results"]["fx_forecast"]]

        return json_response

    @property
    def url_suffix(self) -> str:
        """Url suffix for a given method."""
        return config["url_suffix"]["fx_forecast"]

    @property
    def request(self) -> Dict:
        """Request dictionary FX forecast."""
        currency_pair = self.currency_pair

        request_dict = {
            "currency-pair": currency_pair,
        }

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the json response to a dictionary."""
        _dict: Dict[Any, Any] = {}

        forecast_data = {}

        for fx_type_data in self._data["forecasts"]:
            fx_type_forecast_data = {}
            type = fx_type_data["type"]

            for data in fx_type_data["forecast"]:
                values = {}

                values["Updated_at"] = datetime.strptime(
                    fx_type_data["updated_at"].split("T")[0], "%Y-%m-%d"
                )

                values["Value"] = data["value"]
                fx_type_forecast_data[data["horizon"]] = values

            forecast_data[type] = fx_type_forecast_data

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
            columns={"level_0": "Symbol", "level_1": "FX_type", "level_2": "Horizon"}
        )

        return df
