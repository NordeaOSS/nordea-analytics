import json
from pathlib import Path

from nordea_analytics.nalib.data_retrieval_client import SERVICE_URL
from tests import ProxyFinderFile

DUMP_DATA = False


class TestProxyFinder:
    """Test class for ProxyFinder class."""

    def test_get_proxies_from_file(self) -> None:
        """Test if it can get proxy info from file."""
        proxy_path = Path(__file__).parent / "data" / "proxy_info.txt"
        proxy_finder = ProxyFinderFile(SERVICE_URL, proxy_path=proxy_path)
        assert proxy_finder.proxies == {"http": "this:is:proxy"}

    def test_get_proxies_from_code(self) -> None:
        """Test if proxy info can be retrieved from code."""
        expected_result_file = str(
            (Path(__file__).parent / "data" / "expected_results").joinpath(
                "proxies_from_code_results_"
                + SERVICE_URL.replace(".", "_").replace("://", "_").replace("/", "")
                + ".txt"
            )
        )

        proxy_path: Path = (Path(__file__).parent / "data").joinpath(
            "proxy_info_temp_"
            + SERVICE_URL.replace(".", "_").replace("://", "_").replace("/", "")
            + ".txt"
        )
        proxy_finder = ProxyFinderFile(SERVICE_URL, proxy_path=proxy_path)
        if proxy_path.exists():
            proxy_path.unlink()

        if DUMP_DATA:
            expected_file = open(expected_result_file, "w+")
            json.dump(proxy_finder.proxies, expected_file)

        expected_file = open(expected_result_file, "r")
        expected_results = json.load(expected_file)

        assert proxy_finder.proxies == expected_results

    def test_get_no_proxies_from_code(self) -> None:
        """Test if it throws an error if proxy info can not be found."""
        proxy_path: Path = (Path(__file__).parent / "data").joinpath(
            "proxy_info_temp"
            + SERVICE_URL.replace(".", "_").replace("://", "_").replace("/", "")
            + ".txt"
        )
        try:
            ProxyFinderFile(url="no_results", proxy_path=proxy_path)
            expected_results = False
        except ValueError:
            if proxy_path.exists():
                proxy_path.unlink()
            expected_results = True

        if proxy_path.exists():
            proxy_path.unlink()
        assert expected_results
