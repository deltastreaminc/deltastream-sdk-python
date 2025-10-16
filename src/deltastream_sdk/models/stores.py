"""
Store models for DeltaStream SDK.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from .base import BaseModel, WithClause


@dataclass
class Store(BaseModel):
    """Model representing a DeltaStream data store."""

    parameters: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Store":
        """Create Store instance from dictionary."""
        # Extract base model fields
        base_fields = {}
        parameters = {}

        for field_name, value in data.items():
            field_lower = field_name.lower()

            # Map to base model fields
            if field_lower in ("name", "relation_name", "object_name"):
                base_fields["name"] = value
            elif field_lower in ("created_on", "created_at"):
                base_fields["created_at"] = cls._parse_datetime(value)
            elif field_lower in ("updated_on", "updated_at", "modified_at"):
                base_fields["updated_at"] = cls._parse_datetime(value)
            elif field_lower == "owner":
                base_fields["owner"] = value
            elif field_lower == "comment":
                base_fields["comment"] = value
            else:
                # Store everything else in parameters
                parameters[field_name] = value

        return cls(**base_fields, parameters=parameters)


@dataclass
class StoreCreateParams:
    """Parameters for creating a data store.

    Only 'name' and 'type' are mandatory. All other store-specific parameters
    should be provided in the 'parameters' dictionary.
    """

    name: str
    type: str
    parameters: Optional[Dict[str, Any]] = None

    def to_with_clause(self) -> WithClause:
        """
        Convert parameters to DeltaStream WITH clause.

        Includes type and all parameters in the WITH clause.
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
    comment: Optional[str] = None

    def to_with_clause(self) -> WithClause:
        """Convert update parameters to WITH clause."""
        params = {}

        if self.parameters:
            params.update(self.parameters)

        return WithClause(parameters=params)
