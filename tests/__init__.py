"""Classes in file format for testing."""
from nordea_analytics.nalib.data_retrieval_client import DataRetrievalServiceClient
from nordea_analytics.nalib.util import get_config
from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService

config = get_config()

SERVICE_URL = config["test_service_url"]


class DataRetrievalServiceClientTest(DataRetrievalServiceClient):
    """DataRetrievalServiceClient for testing purposes."""

    def __init__(self) -> None:
        """Initialization of class."""
        if "open" in SERVICE_URL:
            login = True
        else:
            login = False
        super(DataRetrievalServiceClientTest, self).__init__(
            login=login, service_url=SERVICE_URL
        )


class NordeaAnalyticsServiceTest(NordeaAnalyticsService):
    """NordeaAnalyticsService for testing purposes."""

    def __init__(self) -> None:
        """Initialization of the class.

        Gives the Test version of DataRetrievalServiceClient
        as a client to the NordeaAnalyticsService class.
        """
        client = DataRetrievalServiceClientTest()
        NordeaAnalyticsService.__init__(self, client=client)
