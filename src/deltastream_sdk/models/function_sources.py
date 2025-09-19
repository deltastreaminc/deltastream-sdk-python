"""Function Source models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional
from .base import BaseModel


@dataclass
class FunctionSource(BaseModel):
    """Model representing a DeltaStream function source."""

    source_type: Optional[str] = None
    file_path: Optional[str] = None


@dataclass
class FunctionSourceCreateParams:
    """Parameters for creating a function source."""

    name: str
    file_path: str
    comment: Optional[str] = None
