"""Changelog models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional
from .base import BaseModel


@dataclass
class Changelog(BaseModel):
    """Model representing a DeltaStream changelog."""

    status: Optional[str] = None
    sql_definition: Optional[str] = None


@dataclass
class ChangelogCreateParams:
    """Parameters for creating a changelog."""

    name: str
    sql_definition: str
    comment: Optional[str] = None
