# flake8: noqa
from typing import Dict, Optional


class BaseDto(object):
    """A class provides set up its attributes from dictionary."""

    def __init__(self, *args, **kwargs):
        for dictionary in args:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
            setattr(self, key, dictionary[key])


class BackgroundJobInfo(BaseDto):
    """Class represent background job info schema."""

    state: Optional[str] = None
    type: Optional[str] = None
    request_url: Optional[str] = None


class BackgroundJobResponse(BaseDto):
    """Class represent background job response schema."""

    __info: Optional[BackgroundJobInfo] = None
    id: Optional[str] = None

    @property
    def info(self) -> Optional[BackgroundJobInfo]:
        return self.__info

    @info.setter
    def info(self, value) -> None:
        if isinstance(value, dict):
            self.__info = BackgroundJobInfo(value)
        else:
            self.__info = value


class BackgroundJobStatusResponse(BaseDto):
    """Class represent background job status response schema."""

    __info: Optional[BackgroundJobInfo] = None
    response: Optional[Dict] = None

    @property
    def info(self) -> Optional[BackgroundJobInfo]:
        return self.__info

    @info.setter
    def info(self, value) -> None:
        if isinstance(value, dict):
            self.__info = BackgroundJobInfo(value)
        else:
            self.__info = value
