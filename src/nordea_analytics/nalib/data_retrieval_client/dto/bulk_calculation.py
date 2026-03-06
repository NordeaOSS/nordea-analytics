# flake8: noqa
from typing import List

from nordea_analytics.nalib.data_retrieval_client.dto.background import BaseDto


class CalculationStatusDto(BaseDto):
    __job_id: str
    __symbol: str
    __status_code: int
    __status_description: str
    __error: str

    @property
    def job_id(self) -> str:
        return self.__job_id

    @job_id.setter
    def job_id(self, value):
        self.__job_id = value

    @property
    def symbol(self) -> str:
        return self.__symbol

    @symbol.setter
    def symbol(self, value):
        self.__symbol = value

    @property
    def status_code(self) -> int:
        return self.__status_code

    @status_code.setter
    def status_code(self, value):
        self.__status_code = value

    @property
    def status_description(self) -> str:
        return self.__status_description

    @status_description.setter
    def status_description(self, value):
        self.__status_description = value

    @property
    def error(self) -> str:
        return self.__error

    @error.setter
    def error(self, value):
        self.__error = value


class InstrumentCalculationStatusDto(BaseDto):
    __calculations: List[CalculationStatusDto]

    @property
    def calculations(self) -> List[CalculationStatusDto]:
        return self.__calculations

    @calculations.setter
    def calculations(self, value):
        self.__calculations = [CalculationStatusDto(v) for v in value]


class PostBulkCalculationData(BaseDto):
    __standard: InstrumentCalculationStatusDto
    __advanced: InstrumentCalculationStatusDto
    __horizon: InstrumentCalculationStatusDto
    __repo: InstrumentCalculationStatusDto
    __swap_standard: InstrumentCalculationStatusDto
    __swap_horizon: InstrumentCalculationStatusDto
    __id: str

    @property
    def id(self) -> str:
        return self.__id

    @id.setter
    def id(self, value):
        self.__id = value

    @property
    def standard(self) -> InstrumentCalculationStatusDto:
        return self.__standard

    @standard.setter
    def standard(self, value):
        self.__standard = InstrumentCalculationStatusDto(value)

    @property
    def advanced(self) -> InstrumentCalculationStatusDto:
        return self.__advanced

    @advanced.setter
    def advanced(self, value):
        self.__advanced = InstrumentCalculationStatusDto(value)

    @property
    def horizon(self) -> InstrumentCalculationStatusDto:
        return self.__horizon

    @horizon.setter
    def horizon(self, value):
        self.__horizon = InstrumentCalculationStatusDto(value)

    @property
    def repo(self) -> InstrumentCalculationStatusDto:
        return self.__repo

    @repo.setter
    def repo(self, value):
        self.__repo = InstrumentCalculationStatusDto(value)

    @property
    def swap_standard(self) -> InstrumentCalculationStatusDto:
        return self.__swap_standard

    @swap_standard.setter
    def swap_standard(self, value):
        self.__swap_standard = InstrumentCalculationStatusDto(value)

    @property
    def swap_horizon(self) -> InstrumentCalculationStatusDto:
        return self.__swap_horizon

    @swap_horizon.setter
    def swap_horizon(self, value):
        self.__swap_horizon = InstrumentCalculationStatusDto(value)


class BondsBulkCalculationStatusData(BaseDto):
    __id: str
    __state: str
    __underlying_jobs: List[str]
    __new_jobs: List[str]
    __processing_jobs: List[str]
    __completed_jobs: List[str]

    @property
    def id(self) -> str:
        return self.__id

    @id.setter
    def id(self, value):
        self.__id = value

    @property
    def state(self) -> str:
        return self.__state

    @state.setter
    def state(self, value):
        self.__state = value

    @property
    def underlying_jobs(self) -> List[str]:
        return self.__underlying_jobs

    @underlying_jobs.setter
    def underlying_jobs(self, value):
        self.__underlying_jobs = value

    @property
    def new_jobs(self) -> List[str]:
        return self.__new_jobs

    @new_jobs.setter
    def new_jobs(self, value):
        self.__new_jobs = value

    @property
    def processing_jobs(self) -> List[str]:
        return self.__processing_jobs

    @processing_jobs.setter
    def processing_jobs(self, value):
        self.__processing_jobs = value

    @property
    def completed_jobs(self) -> List[str]:
        return self.__completed_jobs

    @completed_jobs.setter
    def completed_jobs(self, value):
        self.__completed_jobs = value
