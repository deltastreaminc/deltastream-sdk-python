"""Entity models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from .base import BaseModel


@dataclass
class Entity(BaseModel):
    """Model representing a DeltaStream entity."""

    entity_type: Optional[str] = None
    schema_definition: Optional[str] = None
    is_leaf: Optional[bool] = (
        None  # Whether this entity is a leaf (can't contain other entities)
    )


@dataclass
class EntityCreateParams:
    """Parameters for creating an entity."""

    name: str
    store: Optional[str] = None
    comment: Optional[str] = None
    params: Optional[Dict[str, Any]] = None  # Parameters like {"topic.partitions": 1}


@dataclass
class EntityUpdateParams:
    """Parameters for updating an entity."""

    schema_definition: Optional[str] = None
    comment: Optional[str] = None
