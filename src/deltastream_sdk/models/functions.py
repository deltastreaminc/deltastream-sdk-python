"""Function models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional
from .base import BaseModel


@dataclass
class Function(BaseModel):
    """Model representing a DeltaStream function."""

    function_type: Optional[str] = None
    language: Optional[str] = None
    definition: Optional[str] = None


@dataclass
class FunctionCreateParams:
    """Parameters for creating a function."""

    name: str
    definition: str
    language: str = "SQL"
    comment: Optional[str] = None
