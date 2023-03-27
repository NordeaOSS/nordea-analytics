"""The Nordea Analytics Python Project API."""
# To distinguish between external and internal packages
try:
    from .nordea import NordeaAnalyticsService  # type: ignore
except (NameError, ModuleNotFoundError):
    from .open_banking import NordeaAnalyticsService  # type: ignore

__all__ = ["NordeaAnalyticsService"]
