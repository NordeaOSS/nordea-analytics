"""Shared fixtures and test configuration."""
from datetime import datetime
from typing import List, Union

from _pytest.config import Config
import pytest

from nordea_analytics import (
    BondKeyFigureName,
    CurveName,
    get_nordea_analytics_test_client,
    NordeaAnalyticsService,
    TimeSeriesKeyFigureName,
)


def pytest_configure(config: Config) -> None:
    """Configure pytest."""
    config.addinivalue_line("markers", "e2e: mark as end-to-end tests.")


@pytest.fixture
def na_service() -> NordeaAnalyticsService:
    """NordeaAnalyticsService test class."""
    return get_nordea_analytics_test_client()


@pytest.fixture
def anchor() -> datetime:
    """Value date for tests."""
    return datetime(2022, 8, 1)


@pytest.fixture
def isins() -> List[str]:
    """ISINs used for testing."""
    return ["DK0002044551", "DE0001141794"]


@pytest.fixture
def bond_curves() -> List[Union[str, CurveName]]:
    """Curves used for testing bond calculation endpoints."""


@pytest.fixture
def isins_partial_spread_data() -> List[str]:
    """ISINs used for testing partial get_bond_key_figures response."""
    return ["DK0009408601", "DK0002030337"]


@pytest.fixture
def keyfigures() -> List[Union[str, BondKeyFigureName]]:
    """Key Figures for BondKeyFigures test."""
    return [
        "Quote",
        "Yield",
        "accint",
        BondKeyFigureName.ModifiedDuration_Deterministic,
        BondKeyFigureName.BPV,
        BondKeyFigureName.CVX,
        BondKeyFigureName.OAModifiedDuration,
    ]


@pytest.fixture
def timeseries_keyfigures() -> List[Union[str, TimeSeriesKeyFigureName]]:
    """Key Figures for TimeSeries tests."""
    return [
        "Quote",
        "Yield",
        "Modduration",
        "accint",
        TimeSeriesKeyFigureName.BPV,
        TimeSeriesKeyFigureName.CVX,
        TimeSeriesKeyFigureName.OAModifiedDuration,
    ]


@pytest.fixture
def index() -> List[str]:
    """Incices for IndexComposition test."""
    return ["DK Mtg Callable", "DK Govt"]


@pytest.fixture
def from_date() -> datetime:
    """From date for Time Series and Curve Time Series test."""
    return datetime(2021, 1, 1)


@pytest.fixture
def tenors() -> List[float]:
    """Tenors for Curve Time Series test."""
    return [0.5, 1]
