from urllib.parse import urljoin, urlparse

import pytest
import requests

from nordea_analytics.nalib.data_retrieval_client import (
    config,
    GET_PROXY_INFO,
    SERVICE_URL,
)
from nordea_analytics.nalib.proxy_finder import ProxyFinder


@pytest.mark.skip("skip test for now")
class TestDataRetrievalServiceClient:
    """Test class for DataRetrievalServiceClient."""

    def test_data_retrieval_service_link(self) -> None:
        """Tests if the given link to the service works.

        Returns: assert statement if the connection to the service is ok.
        """
        service_url_split = urlparse(SERVICE_URL)
        service_url = service_url_split[0] + "://" + service_url_split[1]
        get_proxy_info = GET_PROXY_INFO
        if get_proxy_info:
            proxy_finder = ProxyFinder(service_url)
            proxies = proxy_finder.proxies
            response = requests.get(
                urljoin(service_url, config["url_suffix"]["index_composition"]),
                headers={
                    "X-IBM-client-id": "",
                    "X-IBM-client-secret": "",
                },
                proxies=proxies,
            )

            result = "Unauthorized" in response.text
        else:
            response = requests.head(service_url, verify=False)  # noqa: S501
            result = response.ok

        assert result
