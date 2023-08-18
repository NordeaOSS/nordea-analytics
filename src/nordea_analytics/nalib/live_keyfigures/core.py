from abc import ABC, abstractmethod
from typing import Iterator, List


class HttpStreamIterator(ABC):
    """Http Stream Iterator."""

    @abstractmethod
    def stream(self, bonds: List) -> Iterator[str]:
        """Return Iterator for HTTP stream to get updates from a server.

        Args:
            bonds: List of Bonds to receive updates for.

        Returns: Iterator object that can be used to iterate over HTTP stream.
        """
        pass
