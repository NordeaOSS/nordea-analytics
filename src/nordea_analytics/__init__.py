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
from .shortcuts.utils import disable_analytics_warnings

# To distinguish between external and internal packages
try:
    from .shortcuts.nordea import get_nordea_analytics_client  # type: ignore
    from .shortcuts.nordea import get_nordea_analytics_test_client  # type: ignore # noqa: E401
except (NameError, ModuleNotFoundError):
    from .shortcuts.open_banking import get_nordea_analytics_client  # type: ignore
    from .shortcuts.open_banking import get_nordea_analytics_test_client  # type: ignore # noqa: F401

__version__ = "1.4.0"
__all__ = [
    "get_nordea_analytics_client",
    "get_nordea_analytics_test_client",
    "disable_analytics_warnings",
    "AmortisationType",
    "AssetType",
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
