"""The Nordea Analytics Python Project API."""
from .convention_variable_names import (
    DateRollConvention,
    DayCountConvention,
    Exchange,
    TimeConvention,
)
from .curve_variable_names import CurveDefinitionName, CurveName, CurveType, SpotForward
from .forecast_names import YieldCountry, YieldHorizon, YieldType
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
    Issuers,
)

# To distinguish between external and internal packages
try:
    from .shortcuts.nordea import get_nordea_analytics_client  # type: ignore
except (NameError, ModuleNotFoundError):
    from .shortcuts.open_banking import get_nordea_analytics_client  # type: ignore

__version__ = "1.2.3"
__all__ = [
    "get_nordea_analytics_client",
    "AmortisationType",
    "AssetType",
    "BondKeyFigureName",
    "CalculatedBondKeyFigureName",
    "CapitalCentreTypes",
    "CapitalCentres",
    "CurveDefinitionName",
    "CurveName",
    "CurveType",
    "DateRollConvention",
    "DayCountConvention",
    "Exchange",
    "HorizonCalculatedBondKeyFigureName",
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
