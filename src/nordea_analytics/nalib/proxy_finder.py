import ast
import ctypes.wintypes
from pathlib import Path
import platform
from typing import Any, Dict, Union
import urllib.parse
import urllib.request


class ProxyFinder:
    """Retrieves proxy information when needed."""

    def __init__(self, url: str, proxy_path: Path = None) -> None:
        """Initialization of the class.

        Args:
            url: URL that needs a proxy.
            proxy_path: Path where proxy information is saved.
        """
        self.proxy_path = (
            Path(__file__).parent / ".proxy_info" if proxy_path is None else proxy_path
        )
        self.proxies: Union[Dict, str] = self.get_proxies(url)

    def get_proxies(self, url: str) -> Union[Dict, str]:
        """Find proxy information either from pre-saved place or searched for it.

        Args:
            url: URL that needs a proxy.

        Returns:
            Proxy information.

        Raises:
            ValueError: If proxy information need to be written in manually.
        """
        if self.proxy_path.exists():
            proxy_info = self.proxy_path.read_text()
            return ast.literal_eval(proxy_info)
        else:
            proxy_info = self.find_proxies(url)
            if proxy_info == {} or "http://None" in proxy_info.values():  # type:ignore
                self.proxy_path.write_text(
                    str('{"http":"ENTER PROXY INFO HERE IN A STRING FORMAT"}')
                )
                raise ValueError(
                    "We were not able to find your proxy information, "
                    "please enter them manually in " + str(self.proxy_path)
                )
            else:
                self.proxy_path.write_text(str(proxy_info))
            return proxy_info

    def find_proxies(self, url: str) -> Any:
        """Searches for proxy information.

        Args:
            url: URL that needs a proxy.

        Returns:
            Proxy information.

        Raises:
            Exception: If session can not be closed.
        """
        proxies = urllib.request.getproxies()
        if platform.system() != "Windows":
            return proxies

        session_handle = None
        try:
            ctypes_struct = CtypesStructure(url)
            if ctypes_struct.session_handle == 0 or ctypes_struct.session_handle == -1:
                return {}

            current_user_ie_proxy_config = ctypes_struct.current_user_ie_proxy_config

            if not current_user_ie_proxy_config.fAutoDetect == 1:
                return {}

            auto_proxy_settings = current_user_ie_proxy_config.lpszAutoConfigUrl
            proxy_info = ctypes_struct.proxy_info
            # parse protocol
            parsed_url = urllib.parse.urlparse(url)
            session_handle = ctypes_struct.session_handle
            if "https" in auto_proxy_settings:
                url_prefix = "https"
            else:
                url_prefix = "http"
            return {
                parsed_url.scheme: "{url}://{info}".format(
                    url=url_prefix, info=proxy_info.lpszProxy
                )
            }
        finally:
            if session_handle:
                try:
                    ctypes.windll.winhttp.WinHttpCloseHandle.argtypes = (
                        ctypes.wintypes.HANDLE,
                    )
                    ctypes.windll.winhttp.WinHttpCloseHandle(session_handle)
                except Exception as e:
                    raise e


class CtypesStructure:
    """Find proxy information using the Web Proxy Auto-discovery Protocol."""

    def __init__(self, url: str) -> None:
        """nitialization of the class.

        Args:
            url: URL that needs a proxy.
        """
        # https://docs.microsoft.com/en-us/windows/win32/winhttp/winhttp-autoproxy-api
        self.url = url
        self.winhttp_access_type_no_proxy = 0x00000001
        self.winhttp_no_proxy_name = 0
        self.winhttp_no_proxy_bypass = 0
        self.winhttp_autoproxy_auto_detect = 0x00000001
        self.winhttp_autoproxy_config_url = 0x00000002
        self.winhttp_auto_detect_type_dhcp = 0x00000001
        self.winhttp_auto_detect_type_dns_a = 0x00000002
        self.winHttp = ctypes.windll.winhttp
        self.session_handle = self._get_session_handle()
        self.current_user_ie_proxy_config = self._get_current_user_ie_proxy_config()
        self.autoproxy_options = self._get_autoproxy_options()
        self.proxy_info = self._get_proxy_info()

    def _get_session_handle(self) -> int:
        """Retrieves session handle.

        Returns:
            Session handle.

        """
        # https://docs.microsoft.com/en-gb/windows/win32/api/winhttp/
        # nf-winhttp-winhttpopen
        self.winHttp.WinHttpOpen.restype = ctypes.wintypes.HANDLE
        session_handle = self.winHttp.WinHttpOpen(
            "WinHTTP AutoProxy/1.0",
            self.winhttp_access_type_no_proxy,
            self.winhttp_no_proxy_name,
            self.winhttp_no_proxy_bypass,
            0,
        )
        return session_handle

    def _get_current_user_ie_proxy_config(self) -> Any:
        """Retrieves current user proxy config.

        Returns:
            Current user proxy config.
        """
        # https://docs.microsoft.com/en-us/windows/win32/api/winhttp/
        # ns-winhttp-winhttp_current_user_ie_proxy_config
        class WINHTTP_CURRENT_USER_IE_PROXY_CONFIG(ctypes.Structure):
            _fields_ = [
                ("fAutoDetect", ctypes.wintypes.BOOL),
                ("lpszAutoConfigUrl", ctypes.wintypes.LPWSTR),
                ("lpszProxy", ctypes.wintypes.LPWSTR),
                ("lpszProxyBypass", ctypes.wintypes.LPWSTR),
            ]

        current_user_ie_proxy_config = WINHTTP_CURRENT_USER_IE_PROXY_CONFIG()
        self.winHttp.WinHttpGetIEProxyConfigForCurrentUser(
            ctypes.pointer(current_user_ie_proxy_config)
        )
        return current_user_ie_proxy_config

    def _get_autoproxy_options(self) -> Any:
        """Retrieves auto proxy options.

        Returns:
            Auto proxy options.
        """
        # https://docs.microsoft.com/en-us/windows/win32/api/winhttp/
        # ns-winhttp-winhttp_autoproxy_options
        class WINHTTP_AUTOPROXY_OPTIONS(ctypes.Structure):
            _fields_ = [
                ("dwFlags", ctypes.wintypes.DWORD),
                ("dwAutoDetectFlags", ctypes.wintypes.DWORD),
                ("lpszAutoConfigUrl", ctypes.wintypes.LPCWSTR),
                ("lpvReserved", ctypes.wintypes.LPVOID),
                ("dwReserved", ctypes.wintypes.DWORD),
                ("fAutoLogonIfChallenged", ctypes.wintypes.BOOL),
            ]

        auto_config_url = self.current_user_ie_proxy_config.lpszAutoConfigUrl
        auto_detect = self.current_user_ie_proxy_config.fAutoDetect == 1
        autoproxy_options = WINHTTP_AUTOPROXY_OPTIONS()

        autoproxy_options.dwFlags = (
            self.winhttp_autoproxy_auto_detect if auto_detect else 0
        ) | (self.winhttp_autoproxy_config_url if auto_config_url else 0)
        autoproxy_options.dwAutoDetectFlags = (
            self.winhttp_auto_detect_type_dhcp | self.winhttp_auto_detect_type_dns_a
            if auto_detect
            else 0
        )
        autoproxy_options.lpszAutoConfigUrl = auto_config_url
        autoproxy_options.dwReserved = 0
        autoproxy_options.fAutoLogonIfChallenged = 0
        return autoproxy_options

    def _get_proxy_info(self) -> Any:
        """Retrieves proxy information.

        Returns:
            Proxy info.

        """
        # https://docs.microsoft.com/en-us/windows/win32/api/winhttp/
        # ns-winhttp-winhttp_proxy_info
        class WINHTTP_PROXY_INFO(ctypes.Structure):
            _fields_ = [
                ("dwAccessType", ctypes.wintypes.DWORD),
                ("lpszProxy", ctypes.wintypes.LPWSTR),
                ("lpszProxyBypass", ctypes.wintypes.LPWSTR),
            ]

        proxy_info = WINHTTP_PROXY_INFO()
        self.winHttp.WinHttpGetProxyForUrl.argtypes = (
            ctypes.wintypes.HANDLE,
            ctypes.wintypes.LPCWSTR,
            ctypes.POINTER(self.autoproxy_options.__class__),
            ctypes.POINTER(WINHTTP_PROXY_INFO),
        )
        is_ok = self.winHttp.WinHttpGetProxyForUrl(
            self.session_handle,
            self.url,
            ctypes.pointer(self.autoproxy_options),
            ctypes.pointer(proxy_info),
        )
        # https://docs.microsoft.com/en-us/windows/win32/winhttp/error-messages
        if not is_ok and ctypes.get_last_error() == 12015:
            self.autoproxy_options.fAutoLogonIfChallenged = 1
            is_ok = self.winHttp.WinHttpGetProxyForUrl(
                self.session_handle,
                self.url,
                ctypes.pointer(self.autoproxy_options),
                ctypes.pointer(proxy_info),
            )
            if not is_ok:
                return {}
        return proxy_info
