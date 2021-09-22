"""Shared fixtures and test configuration."""

from _pytest.config import Config


def pytest_configure(config: Config) -> None:
    """Configure pytest."""
    config.addinivalue_line("markers", "e2e: mark as end-to-end tests.")
