"""Function Source models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional
from .base import BaseModel


class FunctionSource(BaseModel):
    """Model representing a DeltaStream function source."""

    @property
    def state(self) -> Optional[str]:
        """Get the state."""
        return self._data.get("State")

    @property
    def language(self) -> Optional[str]:
        """Get the language."""
        return self._data.get("Language")

    @property
    def description(self) -> Optional[str]:
        """Get the description."""
        return self._data.get("Description")

    @property
    def url(self) -> Optional[str]:
        """Get the URL."""
        return self._data.get("Url")


@dataclass
class FunctionSourceCreateParams:
    """Parameters for creating a function source."""

    name: str
    file_path: str
