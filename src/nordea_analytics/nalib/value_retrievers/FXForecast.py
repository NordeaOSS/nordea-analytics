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
    """A class for retrieving and reformatting FX forecasts."""

    def __init__(
        self,
        client: DataRetrievalServiceClient,
        currency_pair: str,
    ) -> None:
        """Initialize the FXForecast object.

        Args:
            client: The client used to retrieve data.
            currency_pair: The currency cross for which to retrieve forecasts.
        """
        super(FXForecast, self).__init__(client)
        self._client = client
        self._currency_pair = currency_pair
        self._data = self.get_fx_forecast()

    def get_fx_forecast(self) -> Mapping:
        """Retrieve response with FX forecast.

        Returns:
            A dictionary containing the FX forecast data.
        """
        json_response = self.get_response(self.request)
        json_response = json_response[config["results"]["fx_forecast"]]
        return json_response

    @property
    def url_suffix(self) -> str:
        """Get URL suffix for the given method.

        Returns:
            The URL suffix for the FX forecast method.
        """
        return config["url_suffix"]["fx_forecast"]

    @property
    def request(self) -> Dict:
        """Get request dictionary for FX forecast.

        Returns:
            The request dictionary for the FX forecast.
        """
        currency_pair = self._currency_pair

        request_dict = {
            "currency-pair": currency_pair,
        }

        return request_dict

    def to_dict(self) -> Dict:
        """Reformat the JSON response to a dictionary.

        Returns:
            The reformatted dictionary from the JSON response.
        """
        forecast_dict: Dict[Any, Any] = {}
        forecast_data = {}

        for fx_type_data in self._data["forecasts"]:
            fx_type_forecast_data = {}
            fx_type = fx_type_data["type"]

            for data in fx_type_data["forecast"]:
                values = {}

                values["Updated_at"] = datetime.strptime(
                    fx_type_data["updated_at"].split("T")[0], "%Y-%m-%d"
                )

                values["Value"] = data["value"]
                fx_type_forecast_data[data["horizon"]] = values

            forecast_data[fx_type] = fx_type_forecast_data

        forecast_dict[self._data["symbol"]] = forecast_data

        return forecast_dict

    def to_df(self) -> pd.DataFrame:
        """Reformat the JSON response to a pandas DataFrame.

        Returns:
            The reformatted DataFrame from the JSON response.
        """
        forecast_dict = self.to_dict()

        df = pd.DataFrame.from_dict(
            {
                (i, j, k): forecast_dict[i][j][k]
                for i in forecast_dict.keys()
                for j in forecast_dict[i].keys()
                for k in forecast_dict[i][j].keys()
            },
            orient="index",
        )
        df = df.reset_index().rename(
            columns={"level_0": "Symbol", "level_1": "FX_type", "level_2": "Horizon"}
        )

        return df
