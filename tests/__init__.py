"""Classes in file format for testing."""
import json
import os
from pathlib import Path
from typing import Any

import yaml

from nordea_analytics.nalib.data_retrieval_client import (
    DataRetrievalServiceClient,
    GET_PROXY_INFO,
)
from nordea_analytics.nalib.proxy_finder import ProxyFinder
from nordea_analytics.nordea_analytics_service import NordeaAnalyticsService

config_path = str(Path(__file__).parent / "config.yml")
with open(config_path) as file:
    config = yaml.safe_load(file)


class DataRetrievalServiceClientFile(DataRetrievalServiceClient):
    """DataRetrievalServiceClient for testing purposes."""

    def __init__(self, file_service: bool, dump_data: bool) -> None:
        """Initialization of class.

        Args:
            file_service: If True, testing data is used
                else the DataRetrievalServiceClient is used.
            dump_data: If True, new testing data created.
        """
        super(DataRetrievalServiceClientFile, self).__init__(login=False)
        self.file_service = file_service
        self.dump_data = dump_data

    def get_response(self, request: dict, url_suffix: str) -> dict:
        """Gets the response from file or from the DataRetrieval service.

         Can also retrieve the data from the DataRetrieval service
        and save it as a new testing data.

        Args:
            request: Request in the form of dictionary.
            url_suffix: Url suffix for a given method.

        Returns:
            Response in the form of json.
        """
        new_string = "_".join(
            "_".join(x) if type(x) is list else x for x in list(request.values())
        )
        file_path = (Path(__file__).parent / "data").joinpath(
            str(url_suffix).replace("/", "_") + "_" + new_string + ".txt"
        )
        new_data = (
            not os.path.isfile(file_path) and not self.file_service
        ) or self.dump_data
        if new_data:
            data_client = DataRetrievalServiceClient()
            json_response = data_client.get_response(request, url_suffix)
            json_file = open(file_path, "w+")
            json.dump(json_response, json_file)
            json_file.close()

            return json_response
        elif not self.file_service:
            data_client = DataRetrievalServiceClient()
            response = data_client.get_response(request, url_suffix)
            return response
        else:
            json_file = open(file_path, "r")
            json_response = json.load(json_file)
            json_file.close()
            return json_response


class NordeaAnalyticsServiceFile(NordeaAnalyticsService):
    """NordeaAnalyticsService for testing purposes."""

    def __init__(self) -> None:
        """Initialization of the class.

        Gives the File/Test version of DataRetrievalServiceClient
        as a client to the NordeaAnalyticsService class.
        """
        self.client = DataRetrievalServiceClientFile(
            config["file_service"], config["dump_data"]
        )
        NordeaAnalyticsService.__init__(self, self.client)


class ProxyFinderFile(ProxyFinder):
    """Retrieves proxy information for testing purposes."""

    def __init__(self, url: str, proxy_path: Path) -> None:
        """Initialization of the class.

        Args:
            url:URL that needs a proxy.
            proxy_path: Path where proxy information is saved.
        """
        self.file_service = config["file_service"]
        self.dump_data = config["dump_data"]
        super(ProxyFinderFile, self).__init__(url=url, proxy_path=proxy_path)

    def find_proxies(self, url: str) -> Any:
        """Searches for proxy information for testing purposes.

        Args:
            url: URL that needs a proxy.

        Returns:
            Proxy information for testing purposes.
        """
        file_path = (Path(__file__).parent / "data").joinpath(
            url.replace(".", "_").replace("://", "_").replace("/", "") + ".txt"
        )
        new_data = (
            not os.path.isfile(file_path) and not self.file_service
        ) or self.dump_data
        if new_data:
            proxy_finder = ProxyFinder(url)
            if GET_PROXY_INFO:
                proxy_info = self._overwrite_real_proxy_info(
                    proxy_finder.find_proxies(url)
                )
            else:
                proxy_info = proxy_finder.find_proxies(url)
            json_file = open(file_path, "w+")
            json.dump(proxy_info, json_file)
            json_file.close()
            return proxy_info
        elif not self.file_service:
            proxy_finder = ProxyFinder(url)
            if GET_PROXY_INFO:
                proxy_info = self._overwrite_real_proxy_info(
                    proxy_finder.find_proxies(url)
                )
            else:
                proxy_info = proxy_finder.find_proxies(url)
            return proxy_info
        else:
            json_file = open(file_path, "r")
            proxy_info = json.load(json_file)
            json_file.close()
            return proxy_info

    @staticmethod
    def _overwrite_real_proxy_info(proxy_info: Any) -> Any:
        """Overwrite real proxy info so that it is not saved in file.

        Args:
            proxy_info: Real proxy info.

        Returns:
            Overwritten proxy data.
        """
        proxy_info["http"] = "http://test.proxy.data.net:0000"
        return proxy_info
