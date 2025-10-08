"""The Nordea Analytics Python Project API."""

from .convention_variable_names import (
    CashflowType,
    DateRollConvention,
    DayCountConvention,
    DmbModel,
    Exchange,
    SwapDayCountConvention,
    SwapLegType,
    SwapFixingFrequency,
    TimeConvention,
)
from .curve_variable_names import (
    CurveDefinitionName,
    CurveName,
    CurveType,
    SpotForward,
    SpotForwardTimeSeries,
)
from .forecast_names import YieldCountry, YieldHorizon, YieldType
from .instrument_groups import InstrumentGroup
from .instrument_variable_names import BenchmarkName, BondIndexName
from .key_figure_names import (
    BondKeyFigureName,
    CalculatedBondKeyFigureName,
    CalculatedRepoBondKeyFigureName,
    HorizonCalculatedBondKeyFigureName,
    LiveBondKeyFigureName,
    SwapHorizonKeyFigureName,
    SwapKeyFigureName,
    TimeSeriesKeyFigureName,
)
from .nordea_analytics_service import NordeaAnalyticsService
from .search_bond_names import (
    AmortisationType,
    AssetType,
    CapitalCentres,
    CapitalCentreTypes,
    SearchBondInstrumentGroup,
    Issuers,
)
from .shortcuts.utils import disable_analytics_warnings

# To distinguish between external and internal packages
__internal_package__ = False
try:
    from .shortcuts.nordea import get_nordea_analytics_client  # type: ignore
    from .shortcuts.nordea import get_nordea_analytics_preprod_client  # type: ignore # noqa: E401

    __internal_package__ = True
except (NameError, ModuleNotFoundError):
    from .shortcuts.open_banking import get_nordea_analytics_client  # type: ignore
    from .shortcuts.open_banking import get_nordea_analytics_preprod_client  # type: ignore # noqa: F401

__version__ = "1.22.5"
__all__ = [
    "get_nordea_analytics_client",
    "get_nordea_analytics_preprod_client",
    "disable_analytics_warnings",
    "AmortisationType",
    "AssetType",
    "BenchmarkName",
    "BondIndexName",
    "BondKeyFigureName",
    "CalculatedBondKeyFigureName",
    "CalculatedRepoBondKeyFigureName",
    "CapitalCentreTypes",
    "CapitalCentres",
    "CashflowType",
    "CurveDefinitionName",
    "CurveName",
    "CurveType",
    "DateRollConvention",
    "DayCountConvention",
    "DmbModel",
    "Exchange",
    "HorizonCalculatedBondKeyFigureName",
    "InstrumentGroup",
    "Issuers",
    "LiveBondKeyFigureName",
    "NordeaAnalyticsService",
    "SearchBondInstrumentGroup",
    "SpotForward",
    "SwapDayCountConvention",
    "SpotForwardTimeSeries",
    "SwapFixingFrequency",
    "SwapHorizonKeyFigureName",
    "SwapKeyFigureName",
    "SwapLegType",
    "TimeConvention",
    "TimeSeriesKeyFigureName",
    "YieldCountry",
    "YieldHorizon",
    "YieldType",
]
