"""
Store models for DeltaStream SDK.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from .base import BaseModel, WithClause


class Store(BaseModel):
    """Model representing a DeltaStream data store."""

    @property
    def parameters(self) -> Dict[str, Any]:
        """Get all store parameters (excluding base fields)."""
        # Return all fields except the base model fields
        # API fields: Name, Type, State, Message, IsDefault, Owner, CreatedAt, UpdatedAt, Path
        base_field_names = (
            "Name",
            "Type",
            "State",
            "Message",
            "IsDefault",
            "Owner",
            "CreatedAt",
            "UpdatedAt",
            "Path",
        )

        return {k: v for k, v in self._data.items() if k not in base_field_names}


@dataclass
class StoreCreateParams:
    """Parameters for creating a data store."""

    name: str
    type: str
    parameters: Optional[Dict[str, Any]] = None

    def to_with_clause(self) -> WithClause:
        """
        Convert parameters to DeltaStream WITH clause.
        """
        params = {"type": self.type}

        # Add all parameters
        if self.parameters:
            params.update(self.parameters)

        return WithClause(parameters=params)


@dataclass
class StoreUpdateParams:
    """Parameters for updating a data store."""

    parameters: Optional[Dict[str, Any]] = None

    def to_with_clause(self) -> WithClause:
        """Convert update parameters to WITH clause."""
        params = {}

        if self.parameters:
            params.update(self.parameters)

        return WithClause(parameters=params)
