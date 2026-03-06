# type: ignore

from datetime import datetime
from typing import Optional, Union

from nordea_analytics.convention_variable_names import (
    SwapDayCountConvention,
    DateRollConvention,
    SwapLegType,
    SwapFixingFrequency,
)


class SwapDefinition:
    """Swap definition."""

    def __init__(
        self,
        currency_paid: str,
        currency_received: str,
        type_paid: Union[str, SwapLegType],
        type_received: Union[str, SwapLegType],
        tenor: Union[str, datetime],
        start: Optional[Union[str, datetime]] = None,
        fix_frequency_paid: Optional[Union[str, SwapFixingFrequency]] = None,
        fix_frequency_received: Optional[Union[str, SwapFixingFrequency]] = None,
        fixed_rate_paid: Optional[float] = None,
        fixed_rate_received: Optional[float] = None,
        floating_spread_paid: Optional[float] = None,
        floating_spread_received: Optional[Union[float, str]] = None,
        day_count_convention_paid: Optional[Union[str, SwapDayCountConvention]] = None,
        day_count_convention_received: Optional[
            Union[str, SwapDayCountConvention]
        ] = None,
        date_roll_convention: Optional[Union[str, DateRollConvention]] = None,
    ) -> None:
        """Initialization of class.

        Args:
            currency_paid: Currency of the paid leg.
            currency_received: Currency of the received leg.
            type_paid: Swap leg type of the paid leg.
            type_received: Swap leg type of the received leg.
            tenor: The tenor of the swap, either generic '10Y' or datetime.
            start: Optional. The start of the swap, either generic '10Y' or datetime.
                Specifying a generic or future datetime makes the swap forward starting
            fix_frequency_paid: Optional. Fixing frequency of the paid leg.
            fix_frequency_received: Optional. Fixing frequency of the received leg.
            fixed_rate_paid: Optional. The fixed rate of the paid leg. Only set if paid leg is fixed.
            fixed_rate_received: Optional. The fixed rate of the received leg. Only set if received leg is fixed.
            floating_spread_paid: Optional. The floating spread of the paid leg. Only set if paid leg is floating.
            floating_spread_received: Optional. The floating spread of the received leg. Only set if received leg is floating.
            day_count_convention_paid: Optional. Day count convention of the paid leg.
            day_count_convention_received: Optional. Day count convention of the received leg.
            date_roll_convention: Optional. Date roll convention of the swap.
        """
        self.currency_paid = currency_paid
        self.currency_received = currency_received
        self.type_paid = (type_paid.value
                          if isinstance(type_paid, SwapLegType)
                          else type_paid)
        self.type_received = (type_received.value
                              if isinstance(type_received, SwapLegType)
                              else type_received)
        self.tenor = tenor
        self.start = start
        self.fix_frequency_paid = (fix_frequency_paid.value
                                   if isinstance(fix_frequency_paid, SwapFixingFrequency)
                                   else fix_frequency_paid)
        self.fix_frequency_received = (fix_frequency_received.value
                                       if isinstance(fix_frequency_received, SwapFixingFrequency)
                                       else fix_frequency_received)
        self.fixed_rate_paid = fixed_rate_paid
        self.fixed_rate_received = fixed_rate_received
        self.floating_spread_paid = floating_spread_paid
        self.floating_spread_received = floating_spread_received
        self.day_count_convention_paid = (day_count_convention_paid.value
                                          if isinstance(day_count_convention_paid, SwapDayCountConvention)
                                          else day_count_convention_paid)
        self.day_count_convention_received = (day_count_convention_received.value
                                              if isinstance(day_count_convention_received, SwapDayCountConvention)
                                              else day_count_convention_received)
        self.date_roll_convention = (date_roll_convention.value
                                     if isinstance(date_roll_convention, DateRollConvention)
                                     else date_roll_convention)

        self.name = "-".join(
            filter(None, [
                self.currency_paid,
                self.currency_received,
                self.type_paid,
                self.type_received,
                self.start,
                self.tenor,
                self.fixed_rate_paid,
                self.fixed_rate_received,
                self.floating_spread_paid,
                self.floating_spread_received,
                self.day_count_convention_paid,
                self.day_count_convention_received,
                self.date_roll_convention,
            ])
        )
