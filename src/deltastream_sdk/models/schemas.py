"""
Schema models for DeltaStream SDK.
"""

from dataclasses import dataclass
from typing import Optional
from .base import BaseModel


class Schema(BaseModel):
    """Model representing a DeltaStream schema."""

    @property
    def is_default(self) -> Optional[bool]:
        """Check if this is the default schema."""
        return self._data.get("IsDefault")


@dataclass
class SchemaCreateParams:
    """Parameters for creating a schema."""

    name: str
