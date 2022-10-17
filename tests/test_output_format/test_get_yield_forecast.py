from datetime import datetime
from typing import Union

from pandas import Timestamp
import pytest

from nordea_analytics import (
    NordeaAnalyticsService,
    YieldCountry,
    YieldHorizon,
    YieldType,
)


class TestYieldForecast:
    """Test class for retrieving Yield forecast data."""

    @pytest.mark.parametrize(
        "country, yield_type, yield_horizon",
        [
            (YieldCountry.DK, YieldType.Govt, YieldHorizon.Horizon_2Y),
        ],
    )
    def test_yield_forecast_dict(
        self,
        na_service: NordeaAnalyticsService,
        country: Union[str, YieldCountry],
        yield_type: Union[str, YieldType],
        yield_horizon: Union[str, YieldHorizon],
    ) -> None:
        """Check if dictionary results are correct."""
        forecast_results = na_service.get_yield_forecasts(
            country=country,
            yield_type=yield_type,
            yield_horizon=yield_horizon,
        )

        country_string = country.value if isinstance(country, YieldCountry) else country
        yield_horizon_string = (
            yield_horizon.value
            if isinstance(yield_horizon, YieldHorizon)
            else yield_horizon
        )
        forecast_key = country_string + " " + yield_horizon_string

        assert forecast_key in forecast_results

        forecast = forecast_results[forecast_key]["Nordea forecast"]
        forecast_updated_at = list(forecast.values())[0]["Updated_at"]
        forecast_value = list(forecast.values())[0]["Value"]

        assert isinstance(forecast_updated_at, datetime)
        assert isinstance(forecast_value, float)

        actual_forecast_tenors = forecast.keys()
        for actual_tenor in actual_forecast_tenors:
            assert isinstance(datetime.strptime(actual_tenor, "%Y-%m-%d"), datetime)

        implied = forecast_results[forecast_key]["Implied"]
        implied_updated_at = list(implied.values())[0]["Updated_at"]
        implied_value = list(implied.values())[0]["Value"]

        assert isinstance(implied_updated_at, datetime)
        assert isinstance(implied_value, float)

        # Only interested in testing the format of the horizon tenors
        expected_implied_horizons = ["Spot", "6M"]
        actual_implied_horizons = implied.keys()
        for expected_horizon in expected_implied_horizons:
            assert expected_horizon in actual_implied_horizons

    @pytest.mark.parametrize(
        "country, yield_type, yield_horizon",
        [
            (YieldCountry.DK, YieldType.Govt, YieldHorizon.Horizon_2Y),
        ],
    )
    def test_yield_forecast_df(
        self,
        na_service: NordeaAnalyticsService,
        country: Union[str, YieldCountry],
        yield_type: Union[str, YieldType],
        yield_horizon: Union[str, YieldHorizon],
    ) -> None:
        """Check if DataFrame results are correct."""
        forecast_results = na_service.get_yield_forecasts(
            country=country,
            yield_type=yield_type,
            yield_horizon=yield_horizon,
            as_df=True,
        )

        country_string = country.value if isinstance(country, YieldCountry) else country
        yield_horizon_string = (
            yield_horizon.value
            if isinstance(yield_horizon, YieldHorizon)
            else yield_horizon
        )
        forecast_key = country_string + " " + yield_horizon_string

        assert forecast_key in list(forecast_results.Symbol)

        forecast = forecast_results[
            forecast_results["Yield_type"].str.contains("Nordea forecast")
        ]
        forecast_updated_at = list(forecast.Updated_at)[0]
        forecast_value = list(forecast.Value)[0]

        assert isinstance(forecast_updated_at, Timestamp)
        assert isinstance(forecast_value, float)

        actual_forecast_horizons = forecast.Horizon
        for actual_horizon in actual_forecast_horizons:
            assert isinstance(datetime.strptime(actual_horizon, "%Y-%m-%d"), datetime)

        implied = forecast_results[
            forecast_results["Yield_type"].str.contains("Implied")
        ]
        implied_updated_at = list(implied.Updated_at)[0]
        implied_value = list(implied.Value)[0]

        assert isinstance(implied_updated_at, Timestamp)
        assert isinstance(implied_value, float)

        # Only interested in testing the format of the horizon tenors
        expected_implied_horizons = ["Spot", "6M"]
        actual_implied_horizons = implied.Horizon
        for expected_horizon in expected_implied_horizons:
            assert list(
                actual_implied_horizons.str.contains(expected_horizon)
            ).__contains__(True)

    @pytest.mark.parametrize(
        "country, yield_type, yield_horizon",
        [
            (YieldCountry.DK, YieldType.Govt, YieldHorizon.Horizon_2Y),
            ("DK", YieldType.Govt, YieldHorizon.Horizon_2Y),
        ],
    )
    def test_yield_forecast_country_inputs(
        self,
        na_service: NordeaAnalyticsService,
        country: Union[str, YieldCountry],
        yield_type: Union[str, YieldType],
        yield_horizon: Union[str, YieldHorizon],
    ) -> None:
        """Check if country input types work as expected."""
        forecast_results = na_service.get_yield_forecasts(
            country=country,
            yield_type=yield_type,
            yield_horizon=yield_horizon,
        )

        country_string = country.value if isinstance(country, YieldCountry) else country
        yield_horizon_string = (
            yield_horizon.value
            if isinstance(yield_horizon, YieldHorizon)
            else yield_horizon
        )
        forecast_key = country_string + " " + yield_horizon_string

        assert forecast_key in forecast_results

        forecast = forecast_results[forecast_key]["Nordea forecast"]
        forecast_updated_at = list(forecast.values())[0]["Updated_at"]
        forecast_value = list(forecast.values())[0]["Value"]

        assert isinstance(forecast_updated_at, datetime)
        assert isinstance(forecast_value, float)

        actual_forecast_tenors = forecast.keys()
        for actual_tenor in actual_forecast_tenors:
            assert isinstance(datetime.strptime(actual_tenor, "%Y-%m-%d"), datetime)

        implied = forecast_results[forecast_key]["Implied"]
        implied_updated_at = list(implied.values())[0]["Updated_at"]
        implied_value = list(implied.values())[0]["Value"]

        assert isinstance(implied_updated_at, datetime)
        assert isinstance(implied_value, float)

        # Only interested in testing the format of the horizon tenors
        expected_implied_horizons = ["Spot", "6M"]
        actual_implied_horizons = implied.keys()
        for expected_horizon in expected_implied_horizons:
            assert expected_horizon in actual_implied_horizons

    @pytest.mark.parametrize(
        "country, yield_type, yield_horizon",
        [
            (YieldCountry.DK, YieldType.Govt, YieldHorizon.Horizon_2Y),
            (YieldCountry.DK, "govt", YieldHorizon.Horizon_2Y),
        ],
    )
    def test_yield_forecast_yield_type_inputs(
        self,
        na_service: NordeaAnalyticsService,
        country: Union[str, YieldCountry],
        yield_type: Union[str, YieldType],
        yield_horizon: Union[str, YieldHorizon],
    ) -> None:
        """Check if country input types work as expected."""
        forecast_results = na_service.get_yield_forecasts(
            country=country,
            yield_type=yield_type,
            yield_horizon=yield_horizon,
        )

        country_string = country.value if isinstance(country, YieldCountry) else country
        yield_horizon_string = (
            yield_horizon.value
            if isinstance(yield_horizon, YieldHorizon)
            else yield_horizon
        )
        forecast_key = country_string + " " + yield_horizon_string

        assert forecast_key in forecast_results

        forecast = forecast_results[forecast_key]["Nordea forecast"]
        forecast_updated_at = list(forecast.values())[0]["Updated_at"]
        forecast_value = list(forecast.values())[0]["Value"]

        assert isinstance(forecast_updated_at, datetime)
        assert isinstance(forecast_value, float)

        actual_forecast_tenors = forecast.keys()
        for actual_tenor in actual_forecast_tenors:
            assert isinstance(datetime.strptime(actual_tenor, "%Y-%m-%d"), datetime)

        implied = forecast_results[forecast_key]["Implied"]
        implied_updated_at = list(implied.values())[0]["Updated_at"]
        implied_value = list(implied.values())[0]["Value"]

        assert isinstance(implied_updated_at, datetime)
        assert isinstance(implied_value, float)

        # Only interested in testing the format of the horizon tenors
        expected_implied_horizons = ["Spot", "6M"]
        actual_implied_horizons = implied.keys()
        for expected_horizon in expected_implied_horizons:
            assert expected_horizon in actual_implied_horizons

    @pytest.mark.parametrize(
        "country, yield_type, yield_horizon",
        [
            (YieldCountry.DK, YieldType.Govt, YieldHorizon.Horizon_2Y),
            (YieldCountry.DK, YieldType.Govt, "2Y"),
        ],
    )
    def test_yield_forecast_yield_horizon_inputs(
        self,
        na_service: NordeaAnalyticsService,
        country: Union[str, YieldCountry],
        yield_type: Union[str, YieldType],
        yield_horizon: Union[str, YieldHorizon],
    ) -> None:
        """Check if country input types work as expected."""
        forecast_results = na_service.get_yield_forecasts(
            country=country,
            yield_type=yield_type,
            yield_horizon=yield_horizon,
        )

        country_string = country.value if isinstance(country, YieldCountry) else country
        yield_horizon_string = (
            yield_horizon.value
            if isinstance(yield_horizon, YieldHorizon)
            else yield_horizon
        )
        forecast_key = country_string + " " + yield_horizon_string

        assert forecast_key in forecast_results

        forecast = forecast_results[forecast_key]["Nordea forecast"]
        forecast_updated_at = list(forecast.values())[0]["Updated_at"]
        forecast_value = list(forecast.values())[0]["Value"]

        assert isinstance(forecast_updated_at, datetime)
        assert isinstance(forecast_value, float)

        actual_forecast_tenors = forecast.keys()
        for actual_tenor in actual_forecast_tenors:
            assert isinstance(datetime.strptime(actual_tenor, "%Y-%m-%d"), datetime)

        implied = forecast_results[forecast_key]["Implied"]
        implied_updated_at = list(implied.values())[0]["Updated_at"]
        implied_value = list(implied.values())[0]["Value"]

        assert isinstance(implied_updated_at, datetime)
        assert isinstance(implied_value, float)

        # Only interested in testing the format of the horizon tenors
        expected_implied_horizons = ["Spot", "6M"]
        actual_implied_horizons = implied.keys()
        for expected_horizon in expected_implied_horizons:
            assert expected_horizon in actual_implied_horizons
