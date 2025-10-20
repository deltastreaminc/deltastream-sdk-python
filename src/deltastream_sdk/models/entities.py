"""Entity models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from .base import BaseModel


class Entity(BaseModel):
    """Model representing a DeltaStream entity."""

    @property
    def is_leaf(self) -> Optional[bool]:
        """Check if this entity is a leaf."""
        return self._data.get("IsLeaf")


@dataclass
class EntityCreateParams:
    """Parameters for creating an entity."""

    name: str
    store: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = (
        None  # Parameters like {"topic.partitions": 1}
    )


@dataclass
class EntityUpdateParams:
    """Parameters for updating an entity."""

    schema_definition: Optional[str] = None
