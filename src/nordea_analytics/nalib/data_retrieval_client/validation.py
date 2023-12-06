from typing import Dict, Optional

from nordea_analytics.nalib.exceptions import CustomWarning, AnalyticsWarning
from nordea_analytics.nalib.http.models import AnalyticsApiResponse


def validate_response(api_response: AnalyticsApiResponse) -> None:
    """Validate for failed queries or calculations and sends warning to user."""
    potential_keys_warnings = ["failed_calculation", "failed_queries"]
    for warning_key in potential_keys_warnings:
        __raise_warnings_for(api_response.data, warning_key)
        __raise_warnings_for(api_response.data_response, warning_key)


def __raise_warnings_for(data: Optional[Dict], key: str) -> None:
    if data is None:
        return

    if key in data:
        failed_queries = data[key]
        if failed_queries:
            for error_message in failed_queries:
                CustomWarning(error_message, AnalyticsWarning)
        else:
            del data[key]
