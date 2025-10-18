"""
Compute Pool models for DeltaStream SDK.
"""

from dataclasses import dataclass
from typing import Optional
from .base import BaseModel, WithClause


class ComputePool(BaseModel):
    """Model representing a DeltaStream compute pool."""

    @property
    def size(self) -> Optional[str]:
        """Get the compute pool size."""
        return self._data.get("Size")

    @property
    def intended_state(self) -> Optional[str]:
        """Get intended state."""
        return self._data.get("IntendedState")

    @property
    def actual_state(self) -> Optional[str]:
        """Get actual state."""
        return self._data.get("ActualState")

    @property
    def error_messages(self) -> Optional[str]:
        """Get error messages."""
        return self._data.get("ErrorMessages")

    @property
    def timeout(self) -> Optional[int]:
        """Get timeout."""
        return self._data.get("Timeout")


@dataclass
class ComputePoolCreateParams:
    """Parameters for creating a compute pool."""

    name: str
    size: str = "SMALL"  # 'SMALL', 'MEDIUM', 'LARGE'
    min_units: int = 1
    max_units: int = 5
    auto_suspend: bool = True
    auto_suspend_minutes: Optional[int] = None

    def to_with_clause(self) -> WithClause:
        """Convert parameters to DeltaStream WITH clause."""
        parameters = {
            "size": self.size,
            "min.units": str(self.min_units),
            "max.units": str(self.max_units),
            "auto.suspend": str(self.auto_suspend).lower(),
        }
        if self.auto_suspend_minutes is not None:
            parameters["auto.suspend.minutes"] = str(self.auto_suspend_minutes)
        return WithClause(parameters=parameters)


@dataclass
class ComputePoolUpdateParams:
    """Parameters for updating a compute pool."""

    size: Optional[str] = None
    min_units: Optional[int] = None
    max_units: Optional[int] = None
    auto_suspend: Optional[bool] = None
    auto_suspend_minutes: Optional[int] = None

    def to_with_clause(self) -> WithClause:
        """Convert update parameters to WITH clause."""
        parameters = {}

        if self.size:
            parameters["size"] = self.size
        if self.min_units is not None:
            parameters["min.units"] = str(self.min_units)
        if self.max_units is not None:
            parameters["max.units"] = str(self.max_units)
        if self.auto_suspend is not None:
            parameters["auto.suspend"] = str(self.auto_suspend).lower()
        if self.auto_suspend_minutes is not None:
            parameters["auto.suspend.minutes"] = str(self.auto_suspend_minutes)

        return WithClause(parameters=parameters)
