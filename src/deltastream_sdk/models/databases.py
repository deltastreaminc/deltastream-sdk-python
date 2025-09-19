"""
Database models for DeltaStream SDK.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from .base import BaseModel


@dataclass
class Database(BaseModel):
    """Model representing a DeltaStream database."""

    # Database properties
    database_type: Optional[str] = None
    status: Optional[str] = None
    is_default: Optional[bool] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Database":
        """Create Database instance from dictionary."""
        mapped_data = {}

        for field_name, value in data.items():
            field_lower = field_name.lower()

            if field_lower in ("type", "database_type"):
                mapped_data["database_type"] = value
            elif field_lower in ("status", "state"):
                mapped_data["status"] = value
            elif field_lower in ("is_default", "default"):
                mapped_data["is_default"] = value
            else:
                mapped_data[field_lower] = value

        # Use parent class mapping for common fields
        base_data = super().from_dict(data).to_dict()
        mapped_data.update(base_data)

        # Filter to only include fields that exist in this dataclass
        field_names = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in mapped_data.items() if k in field_names}

        return cls(**filtered_data)


@dataclass
class DatabaseCreateParams:
    """Parameters for creating a database."""

    name: str
    comment: Optional[str] = None
