"""
Schema models for DeltaStream SDK.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from .base import BaseModel


@dataclass
class Schema(BaseModel):
    """Model representing a DeltaStream schema."""

    # Schema properties
    is_default: Optional[bool] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Schema":
        """Create Schema instance from dictionary."""
        # Use parent class mapping for common fields first
        base_data = super().from_dict(data).to_dict()
        mapped_data = base_data.copy()

        # Apply child class specific mappings (will override base mappings if there are conflicts)
        for field_name, value in data.items():
            field_lower = field_name.lower()

            if field_lower in ("is_default", "default", "is default"):
                mapped_data["is_default"] = value

        # Filter to only include fields that exist in this dataclass
        field_names = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in mapped_data.items() if k in field_names}

        return cls(**filtered_data)


@dataclass
class SchemaCreateParams:
    """Parameters for creating a schema."""

    name: str
    comment: Optional[str] = None
