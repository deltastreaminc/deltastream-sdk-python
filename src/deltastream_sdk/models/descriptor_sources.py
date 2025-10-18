"""Descriptor Source models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional, List
from .base import BaseModel


class DescriptorSource(BaseModel):
    """Model representing a DeltaStream descriptor source."""

    @property
    def source_type(self) -> Optional[str]:
        """Get the source type."""
        return self._data.get("Type")

    @property
    def url(self) -> Optional[str]:
        """Get the URL."""
        return self._data.get("Url")

    @property
    def tags(self) -> Optional[List[str]]:
        """Get the tags."""
        return self._data.get("Tags")


@dataclass
class DescriptorSourceCreateParams:
    """Parameters for creating a descriptor source."""

    name: str
    file_path: str
