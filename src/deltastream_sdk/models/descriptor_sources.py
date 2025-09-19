"""Descriptor Source models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional
from .base import BaseModel


@dataclass
class DescriptorSource(BaseModel):
    """Model representing a DeltaStream descriptor source."""

    source_type: Optional[str] = None
    file_path: Optional[str] = None


@dataclass
class DescriptorSourceCreateParams:
    """Parameters for creating a descriptor source."""

    name: str
    file_path: str
    comment: Optional[str] = None
