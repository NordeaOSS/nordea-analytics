from nordea_analytics.nalib.exceptions import AnalyticsWarning


def disable_analytics_warnings() -> None:
    """Disable AnalyticsWarning warnings."""
    import warnings

    warnings.filterwarnings("ignore", category=AnalyticsWarning)
