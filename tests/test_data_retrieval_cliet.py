import requests

from nordea_analytics.nalib.data_retrieval_client import SERVICE_URL


class TestDataRetrievalServiceClient:
    """Test ckass for DataRetrievalServiceClient."""

    def test_data_retrival_service_link(self) -> None:
        """Tests if the given link to the service works.

        Returns: assert statement if the connection to the service is ok.
        """
        service_url = SERVICE_URL
        response = requests.head(service_url)
        assert response.ok
