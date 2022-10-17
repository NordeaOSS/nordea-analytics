from datetime import datetime

from pandas import Timestamp
import pytest

from nordea_analytics import NordeaAnalyticsService


class TestFXForecast:
    """Test class for retrieving FX forecast data."""

    @pytest.mark.parametrize(
        "currency_pair",
        [
            "USDdkk",
        ],
    )
    def test_fx_forecast_dict(
        self,
        na_service: NordeaAnalyticsService,
        currency_pair: str,
    ) -> None:
        """Check if dictionary results are correct."""
        forecast_results = na_service.get_fx_forecasts(currency_pair=currency_pair)

        assert currency_pair in forecast_results

        forecast = forecast_results[currency_pair]["Nordea forecast"]
        forecast_updated_at = list(forecast.values())[0]["Updated_at"]
        forecast_value = list(forecast.values())[0]["Value"]

        assert type(forecast_updated_at) == datetime
        assert type(forecast_value) == float

        actual_forecast_tenors = forecast.keys()
        for actual_tenor in actual_forecast_tenors:
            assert type(datetime.strptime(actual_tenor, "%Y-%m-%d")) == datetime

        implied = forecast_results[currency_pair]["Implied"]
        implied_updated_at = list(implied.values())[0]["Updated_at"]
        implied_value = list(implied.values())[0]["Value"]

        assert type(implied_updated_at) == datetime
        assert type(implied_value) == float

        # Only interested in testing the format of the horizon tenors
        expected_implied_horizons = ["Spot", "6M"]
        actual_implied_horizons = implied.keys()
        for expected_horizon in expected_implied_horizons:
            assert expected_horizon in actual_implied_horizons

    @pytest.mark.parametrize(
        "currency_pair",
        [
            "USDDKK",
        ],
    )
    def test_fx_forecast_df(
        self,
        na_service: NordeaAnalyticsService,
        currency_pair: str,
    ) -> None:
        """Check if DataFrame results are correct."""
        forecast_results = na_service.get_fx_forecasts(
            currency_pair=currency_pair, as_df=True
        )

        assert currency_pair in list(forecast_results.Symbol)

        forecast = forecast_results[
            forecast_results["FX_type"].str.contains("Nordea forecast")
        ]
        forecast_updated_at = list(forecast.Updated_at)[0]
        forecast_value = list(forecast.Value)[0]

        assert type(forecast_updated_at) == Timestamp
        assert type(forecast_value) == float

        actual_forecast_horizons = forecast.Horizon
        for actual_horizon in actual_forecast_horizons:
            assert type(datetime.strptime(actual_horizon, "%Y-%m-%d")) == datetime

        implied = forecast_results[forecast_results["FX_type"].str.contains("Implied")]
        implied_updated_at = list(implied.Updated_at)[0]
        implied_value = list(implied.Value)[0]

        assert type(implied_updated_at) == Timestamp
        assert type(implied_value) == float

        # Only interested in testing the format of the horizon tenors
        expected_implied_horizons = ["Spot", "6M"]
        actual_implied_horizons = implied.Horizon
        for expected_horizon in expected_implied_horizons:
            assert list(
                actual_implied_horizons.str.contains(expected_horizon)
            ).__contains__(True)
