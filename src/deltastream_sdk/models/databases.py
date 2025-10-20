"""
Database models for DeltaStream SDK.
"""

from dataclasses import dataclass
from typing import Optional
from .base import BaseModel


class Database(BaseModel):
    """Model representing a DeltaStream database."""

    @property
    def is_default(self) -> Optional[bool]:
        """Check if this is the default database."""
        return self._data.get("IsDefault")


@dataclass
class DatabaseCreateParams:
    """Parameters for creating a database."""

    name: str
