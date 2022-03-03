from abc import ABC
import logging
from pathlib import Path

import easygui
from keyring import get_password, set_password

from nordea_analytics.nalib.util import get_user

logger = logging.getLogger(__name__)


class Login(ABC):
    """Looks for username and password for a given service_name."""

    def __init__(
        self,
        service_name: str,
        username: str = None,
        password: str = None,
        new_credentials: bool = False,
    ) -> None:
        """Initialization of class.

        If username and password is None, it looks for Windows username
        and asks for password in a pop-up window.

        Args:
            service_name: key in the credentials manager.
            username: Windows g-login or client id(external users).
            password: Windows password or client secret(external users).
            new_credentials: If credentials need to be reset
        """
        self.user_path = Path(__file__).parent / (".user_info_" + service_name)
        self._service_name = service_name
        self.username = username if username else get_user(self.user_path)
        self.new_credentials = new_credentials

        if new_credentials or self.username == "":
            self.set_new_user_and_password()
        elif password:
            self.password = password
        else:
            self._get_password()

    def save_login(self) -> None:
        """Saves login credentials in keyring and user_info file."""
        set_password(self._service_name, self.username, self.password)
        self.user_path.write_text(str(self.username))
        logger.info(
            "Password set for {} and user {}".format(self._service_name, self.username)
        )

    def set_new_user_and_password(self) -> None:
        """Asks for new password if needed and saves it."""
        if self.new_credentials:
            title = "WRONG or EXPIRED USER or PASSWORD."
            msg = (
                f"Wrong or expired user or password. Please enter your "
                f"user and password for {self._service_name}"
            )
        else:
            title = "Enter password"
            msg = f"Please enter your credentials for {self._service_name}"
        fields = ["Username/Client ID", "Password/Client Secret"]
        defaults = ["", ""]
        self.username, self.password = easygui.multpasswordbox(
            msg, title=title, fields=fields, values=defaults
        )
        self.save_login()

    def _get_password(self) -> str:
        """Looks for the password in keyring, and if not found asks for a new password.

        Returns:
             Password for a given user.
        """
        password = get_password(self._service_name, self.username)
        if password:
            logger.info(
                f"Password retrieved from the credentials manager "
                f"for {self._service_name} and user {self.username}"
            )
            self.password = password
        else:
            logger.info(
                f"Could not retrieved password from the credentials manager"
                f" for {self._service_name} and user {self.username}."
                f" Trying to set new password."
            )
            self.set_new_user_and_password()

        return self.password
