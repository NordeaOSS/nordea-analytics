"""The Nordea Analytics Python Project API."""
from .convention_variable_names import (
    CashflowType,
    DateRollConvention,
    DayCountConvention,
    Exchange,
    TimeConvention,
)
from .curve_variable_names import CurveDefinitionName, CurveName, CurveType, SpotForward
from .forecast_names import YieldCountry, YieldHorizon, YieldType
from .instrument_variable_names import BenchmarkName, BondIndexName
from .key_figure_names import (
    BondKeyFigureName,
    CalculatedBondKeyFigureName,
    HorizonCalculatedBondKeyFigureName,
    LiveBondKeyFigureName,
    TimeSeriesKeyFigureName,
)
from .nordea_analytics_service import NordeaAnalyticsService
from .search_bond_names import (
    AmortisationType,
    AssetType,
    CapitalCentres,
    CapitalCentreTypes,
    InstrumentGroup,
    Issuers,
)
from .shortcuts.utils import disable_analytics_warnings

from .shortcuts.open_banking import get_nordea_analytics_client
from .shortcuts.open_banking import get_nordea_analytics_test_client

__version__ = "1.14.0"
__all__ = [
    "get_nordea_analytics_client",
    "get_nordea_analytics_test_client",
    "disable_analytics_warnings",
    "AmortisationType",
    "AssetType",
    "BenchmarkName",
    "BondIndexName",
    "BondKeyFigureName",
    "CalculatedBondKeyFigureName",
    "CapitalCentreTypes",
    "CapitalCentres",
    "CashflowType",
    "CurveDefinitionName",
    "CurveName",
    "CurveType",
    "DateRollConvention",
    "DayCountConvention",
    "Exchange",
    "HorizonCalculatedBondKeyFigureName",
    "InstrumentGroup",
    "Issuers",
    "LiveBondKeyFigureName",
    "NordeaAnalyticsService",
    "SpotForward",
    "TimeConvention",
    "TimeSeriesKeyFigureName",
    "YieldCountry",
    "YieldHorizon",
    "YieldType",
]
