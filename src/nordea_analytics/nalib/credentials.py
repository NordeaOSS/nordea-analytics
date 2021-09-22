from abc import ABC
from getpass import getuser
import logging

import easygui
from keyring import get_password, set_password

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
            username: Windows g-login.
            password: Windows password.
            new_credentials: If credentials need to be reset
        """
        self._service_name = service_name
        self.username = username if username else getuser()
        self.new_credentials = new_credentials

        if new_credentials:
            self.set_new_password()
        elif password:
            self.password = password
        else:
            self._get_password()

    def save_login(self) -> None:
        """Saves login credentials in keyring."""
        set_password(self._service_name, self.username, self.password)
        logger.info(
            "Password set for service {} and user {}".format(
                self._service_name, self.username
            )
        )

    def set_new_password(self) -> None:
        """Asks for new password if needed and saves it."""
        if self.new_credentials:
            title = "WRONG or EXPIRED PASSWORD."
            msg = (
                f"Wrong or expired password. Please enter your password for the "
                f"user {self.username} on database {self._service_name}"
            )
        else:
            title = "Enter password"
            msg = (
                f"Please enter your password for the user {self.username}"
                f" on database {self._service_name}"
            )
        fields = ["Username", "Password"]
        defaults = [self.username, ""]
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
                f"for database {self._service_name} and user {self.username}"
            )
            self.password = password
        else:
            logger.info(
                f"Could not retrieved password from the credentials manager"
                f" for database {self._service_name} and user {self.username}."
                f" Trying to set new password."
            )
            self.set_new_password()

        return self.password
