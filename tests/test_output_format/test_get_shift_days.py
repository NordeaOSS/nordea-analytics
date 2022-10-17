from datetime import datetime
from typing import Union

import pytest

from nordea_analytics import (
    DateRollConvention,
    DayCountConvention,
    Exchange,
    NordeaAnalyticsService,
)


class TestShiftDays:
    """Test class for shifting a datetime."""

    @pytest.mark.parametrize(
        "days",
        [
            2,
        ],
    )
    def test_shift_days_only_required_arguments(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        days: int,
    ) -> None:
        """Check if result is correct."""
        shifted_day = na_service.get_shift_days(
            date=anchor,
            days=days,
        )

        assert type(shifted_day) == datetime

    @pytest.mark.parametrize(
        "days, exchange",
        [
            (
                2,
                Exchange.Copenhagen,
            ),
            (
                2,
                "dkK",
            ),
        ],
    )
    def test_shift_days_exchange_input_types(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        days: int,
        exchange: Union[str, Exchange],
    ) -> None:
        """Check if exchange input types work as expected."""
        shifted_day = na_service.get_shift_days(
            date=anchor, days=days, exchange=exchange
        )

        assert type(shifted_day) == datetime

    @pytest.mark.parametrize(
        "days, day_count_convention",
        [
            (
                2,
                DayCountConvention.BusinessDays,
            ),
            (
                2,
                "business DAYS",
            ),
        ],
    )
    def test_shift_days_day_count_convention_input_types(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        days: int,
        day_count_convention: Union[str, DayCountConvention],
    ) -> None:
        """Check if day count convention input types work as expected."""
        shifted_day = na_service.get_shift_days(
            date=anchor, days=days, day_count_convention=day_count_convention
        )

        assert type(shifted_day) == datetime

    @pytest.mark.parametrize(
        "days, date_roll_convention",
        [
            (
                2,
                DateRollConvention.Preceeding,
            ),
            (
                2,
                "preCEEDING",
            ),
        ],
    )
    def test_shift_days_date_roll_convention_input_types(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        days: int,
        date_roll_convention: Union[str, DateRollConvention],
    ) -> None:
        """Check if date roll convention input types work as expected."""
        shifted_day = na_service.get_shift_days(
            date=anchor, days=days, date_roll_convention=date_roll_convention
        )

        assert type(shifted_day) == datetime
