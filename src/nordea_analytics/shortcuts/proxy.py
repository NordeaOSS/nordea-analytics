from typing import Dict

from nordea_analytics.nalib.proxy_finder import ProxyFinder


def find_proxy(url: str) -> Dict:
    """Search for appropriate proxy server."""
    try:
        return ProxyFinder.find_proxies(url)
    except Exception:
        return {}
