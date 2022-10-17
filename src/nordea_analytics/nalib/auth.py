"""Script for various methods for auth nordea analytics library."""
from urllib.parse import urljoin, urlparse

import requests
from requests import cookies
from requests_kerberos import DISABLED, HTTPKerberosAuth  # type: ignore

from nordea_analytics.nalib.util import get_config

config = get_config()


def authenticate() -> cookies.RequestsCookieJar:
    """Function provide OAUTH2 authentication.

    Returns:
        Auth cookies if authentication succeed.
        None if OAUTH2 authentication method is not supported.

    """
    if "auth" not in config["url_suffix"]:
        return None

    base_url = config["service_url"]
    auth_url = urljoin(base_url, config["url_suffix"]["auth"])
    base_domain = urlparse(base_url).netloc
    kerberos_auth = HTTPKerberosAuth(
        mutual_authentication=DISABLED, sanitize_mutual_error_response=False
    )
    with requests.session() as http_session:
        response = http_session.get(auth_url, auth=kerberos_auth, allow_redirects=True)
        response.raise_for_status()

        # Clear cookies from different from base_domain domains
        for domain in http_session.cookies.list_domains():
            if domain != base_domain:
                http_session.cookies.clear(domain)

        return http_session.cookies
