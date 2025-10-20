"""Changelog models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional
from .base import BaseModel


class Changelog(BaseModel):
    """Model representing a DeltaStream changelog."""

    @property
    def state(self) -> Optional[str]:
        """Get the changelog state."""
        return self._data.get("State")


@dataclass
class ChangelogCreateParams:
    """Parameters for creating a changelog."""

    name: str
    sql_definition: str
