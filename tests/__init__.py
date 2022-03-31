"""Classes in file format for testing."""
from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
    LiveDataRetrievalServiceClient,
)
from nordea_analytics.nalib.util import get_config
from nordea_analytics.nordea_analytics_service import (
    NordeaAnalyticsLiveService,
    NordeaAnalyticsService,
)

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


class LiveDataRetrievalServiceClientTest(LiveDataRetrievalServiceClient):
    """DataRetrievalServiceClient for testing purposes."""

    def __init__(self, streaming: bool = False) -> None:
        """Initialization of class.

        Args:
            streaming: boolean if should use streaming. Used for testing.
        """
        if "open" in SERVICE_URL:
            login = True
        else:
            login = False
        super(LiveDataRetrievalServiceClientTest, self).__init__(
            login=login, service_url=SERVICE_URL, streaming=streaming
        )


class NordeaAnalyticsLiveServiceTest(NordeaAnalyticsLiveService):
    """NordeaAnalyticsService for testing purposes."""

    def __init__(self, streaming: bool = False) -> None:
        """Initialization of the class.

        Gives the Test version of DataRetrievalServiceClient
        as a client to the NordeaAnalyticsService class.

        Args:
            streaming: boolean if should use streaming. Used for testing.
        """
        client = LiveDataRetrievalServiceClientTest(streaming)
        NordeaAnalyticsLiveService.__init__(self, client=client)
