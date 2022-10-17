from datetime import datetime, timedelta
from typing import Union

import pytest

from nordea_analytics import (
    NordeaAnalyticsService,
    TimeConvention,
)


class TestYearFraction:
    """Test class for calculating a year fraction."""

    @pytest.mark.parametrize(
        "days, time_convention",
        [
            (
                365,
                TimeConvention.Act365,
            ),
            (
                100,
                TimeConvention.Act365,
            ),
            (
                100,
                "aCT365",
            ),
        ],
    )
    def test_year_fraction(
        self,
        na_service: NordeaAnalyticsService,
        anchor: datetime,
        days: int,
        time_convention: Union[TimeConvention, str],
    ) -> None:
        """Check if result is correct."""
        time_convention_string = (
            time_convention[0] if type(time_convention) == tuple else time_convention
        )
        year_fraction = na_service.get_year_fraction(
            from_date=anchor,
            to_date=anchor + timedelta(days=days),
            time_convention=time_convention_string,
        )

        isFloat = isinstance(year_fraction, float)
        isInt = isinstance(year_fraction, int)

        assert isFloat or isInt
