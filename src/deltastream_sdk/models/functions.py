"""Function models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from .base import BaseModel


class Function(BaseModel):
    """Model representing a DeltaStream function."""

    @property
    def name(self) -> str:
        """Get the function signature (which serves as the name)."""
        return self._data.get("Signature", "")

    @property
    def function_type(self) -> Optional[str]:
        """Get the function type."""
        return self._data.get("Type")

    @property
    def source_name(self) -> Optional[str]:
        """Get the source name."""
        return self._data.get("SourceName")

    @property
    def class_name(self) -> Optional[str]:
        """Get the class name."""
        return self._data.get("ClassName")

    @property
    def egress_allow_uris(self) -> Optional[List[str]]:
        """Get the egress allow URIs."""
        return self._data.get("EgressAllowURIs")

    @property
    def properties(self) -> Optional[Dict[str, Any]]:
        """Get the properties."""
        return self._data.get("Properties")


@dataclass
class FunctionCreateParams:
    """Parameters for creating a function."""

    name: str
    definition: str
    language: str = "SQL"
