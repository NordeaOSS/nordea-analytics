import os
from typing import Dict

from nordea_analytics import NordeaAnalyticsService
from nordea_analytics.nalib.data_retrieval_client import DataRetrievalServiceClient
from nordea_analytics.nalib.http.open_banking import (
    OpenBankingClientConfiguration,
    OpenBankingHttpClient,
)
from nordea_analytics.nalib.live_keyfigures.open_banking import (
    OpenBankingHttpStreamIterator,
)
from nordea_analytics.shortcuts.proxy import find_proxy  # type: ignore

DEFAULT_URL = "https://open.nordea.com/instrument-analytics/v1/"


def get_nordea_analytics_client(
    client_id: str, client_secret: str, base_url: str = None, use_proxy: bool = False
) -> NordeaAnalyticsService:
    """Shortcut function to create :class:`NordeaAnalyticsService`.

    Remarks:
        The Client ID and Client Secret are generated after the application is created on My apps page.
        The Client ID and Client Secret can be found when clicking the app icon after the app is created.

    Args:
        client_id:
            client id is required.
        client_secret:
            client secret is required.
        base_url:
            (optional) base url of Nordea Analytics Service.
        use_proxy:
            (optional) Search for appropriate proxy server if it set.

    Returns:
        Service client which can be used to retrieve data.
    """
    proxies = {}
    headers: Dict = {}
    base_url = base_url or DEFAULT_URL
    if use_proxy:
        proxies = find_proxy(base_url)

    configuration = OpenBankingClientConfiguration(
        base_url,
        client_id=client_id,
        client_secret=client_secret,
        proxies=proxies,
        headers=headers,
    )
    http_client = OpenBankingHttpClient(configuration)

    data_retrieval_service_client = DataRetrievalServiceClient(
        http_client, OpenBankingHttpStreamIterator(http_client)
    )

    return NordeaAnalyticsService(data_retrieval_service_client)


def get_nordea_analytics_test_client(
    base_url: str = None, use_proxy: bool = False
) -> NordeaAnalyticsService:
    """Create and return test instance of NordeaAnalyticsService."""
    client_id = os.getenv("clientId")
    client_secret = os.getenv("clientSecret")
    if client_id is None or client_secret is None:
        raise ValueError("Test user credentials are not set")

    return get_nordea_analytics_client(client_id, client_secret, base_url, use_proxy)
