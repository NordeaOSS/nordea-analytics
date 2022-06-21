from pathlib import Path

import pytest

from nordea_analytics.nalib.proxy_finder import ProxyFinder

DUMP_DATA = False

SERVICE_URL = "https://open.nordea.com/instrument-analytics/v1/"
# Proxies only needed for OB link


@pytest.mark.skip("Skip test, fails in bamboo")
class TestProxyFinder:
    """Test class for ProxyFinder class."""

    def test_get_proxies_from_file(self) -> None:
        """Test if it can get proxy info from file."""
        proxy_path = Path(__file__).parent / "data" / "proxy_info.txt"
        proxy_finder = ProxyFinder(SERVICE_URL, proxy_path=proxy_path)
        assert proxy_finder.proxies == {"http": "this:is:proxy"}

    def test_get_proxies_from_code(self) -> None:
        """Test if proxy info can be retrieved from code."""
        proxy_path: Path = (Path(__file__).parent / "data").joinpath(
            "proxy_info_temp.txt"
        )
        if proxy_path.exists():
            proxy_path.unlink()
        proxy_finder = ProxyFinder(SERVICE_URL, proxy_path=proxy_path)
        if proxy_path.exists():
            proxy_path.unlink()

        assert (proxy_finder.proxies["https"] != "") or (  # type:ignore
            proxy_finder.proxies["http"] != ""  # type:ignore
        )

    def test_get_no_proxies_from_code(self) -> None:
        """Test if it throws an error if proxy info can not be found."""
        proxy_path: Path = (Path(__file__).parent / "data").joinpath(
            "proxy_info_return_error_temp.txt"
        )
        if proxy_path.exists():
            proxy_path.unlink()
        try:
            ProxyFinder(url="no_results", proxy_path=proxy_path)
            expected_results = False
        except ValueError:
            if proxy_path.exists():
                proxy_path.unlink()
            expected_results = True

        if proxy_path.exists():
            proxy_path.unlink()
        assert expected_results
