"""
Compute Pool models for DeltaStream SDK.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from .base import BaseModel, WithClause


@dataclass
class ComputePool(BaseModel):
    """Model representing a DeltaStream compute pool."""

    # Pool configuration
    size: Optional[str] = None  # 'SMALL', 'MEDIUM', 'LARGE'
    min_units: Optional[int] = None
    max_units: Optional[int] = None
    current_units: Optional[int] = None

    # Status and state
    status: Optional[str] = None  # 'RUNNING', 'STOPPED', 'STARTING', 'STOPPING'
    auto_suspend: Optional[bool] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ComputePool":
        """Create ComputePool instance from dictionary."""
        # Start with parent class mapping for common fields
        base_data = super().from_dict(data).to_dict()
        mapped_data = base_data.copy()

        for field_name, value in data.items():
            field_lower = field_name.lower()

            if field_lower == "size":
                mapped_data["size"] = value
            elif field_lower in ("min_units", "minimum_units"):
                mapped_data["min_units"] = (
                    int(value) if isinstance(value, str) and value.isdigit() else value
                )
            elif field_lower in ("max_units", "maximum_units"):
                mapped_data["max_units"] = (
                    int(value) if isinstance(value, str) and value.isdigit() else value
                )
            elif field_lower in ("current_units", "active_units"):
                mapped_data["current_units"] = (
                    int(value) if isinstance(value, str) and value.isdigit() else value
                )
            elif field_lower in ("status", "state"):
                mapped_data["status"] = value
            elif field_lower == "auto_suspend":
                if isinstance(value, str):
                    mapped_data["auto_suspend"] = value.lower() in (
                        "true",
                        "1",
                        "yes",
                        "on",
                    )
                else:
                    mapped_data["auto_suspend"] = value
            else:
                mapped_data[field_lower] = value

        # Filter to only include fields that exist in this dataclass
        field_names = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in mapped_data.items() if k in field_names}

        return cls(**filtered_data)


@dataclass
class ComputePoolCreateParams:
    """Parameters for creating a compute pool."""

    name: str
    size: str = "SMALL"  # 'SMALL', 'MEDIUM', 'LARGE'
    min_units: int = 1
    max_units: int = 5
    auto_suspend: bool = True
    auto_suspend_minutes: Optional[int] = None
    comment: Optional[str] = None

    def to_with_clause(self) -> WithClause:
        """Convert parameters to DeltaStream WITH clause."""
        params = {
            "size": self.size,
            "min.units": str(self.min_units),
            "max.units": str(self.max_units),
            "auto.suspend": str(self.auto_suspend).lower(),
        }
        if self.auto_suspend_minutes is not None:
            params["auto.suspend.minutes"] = str(self.auto_suspend_minutes)
        return WithClause(parameters=params)


@dataclass
class ComputePoolUpdateParams:
    """Parameters for updating a compute pool."""

    size: Optional[str] = None
    min_units: Optional[int] = None
    max_units: Optional[int] = None
    auto_suspend: Optional[bool] = None
    auto_suspend_minutes: Optional[int] = None
    comment: Optional[str] = None

    def to_with_clause(self) -> WithClause:
        """Convert update parameters to WITH clause."""
        params = {}

        if self.size:
            params["size"] = self.size
        if self.min_units is not None:
            params["min.units"] = str(self.min_units)
        if self.max_units is not None:
            params["max.units"] = str(self.max_units)
        if self.auto_suspend is not None:
            params["auto.suspend"] = str(self.auto_suspend).lower()
        if self.auto_suspend_minutes is not None:
            params["auto.suspend.minutes"] = str(self.auto_suspend_minutes)

        return WithClause(parameters=params)
