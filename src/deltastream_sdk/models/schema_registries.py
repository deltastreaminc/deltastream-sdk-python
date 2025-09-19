"""Schema Registry models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional
from .base import BaseModel, WithClause


@dataclass
class SchemaRegistry(BaseModel):
    """Model representing a DeltaStream schema registry."""

    registry_type: Optional[str] = None
    url: Optional[str] = None
    auth_type: Optional[str] = None


@dataclass
class SchemaRegistryCreateParams:
    """Parameters for creating a schema registry."""

    name: str
    url: str
    auth_type: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    comment: Optional[str] = None

    def to_with_clause(self) -> WithClause:
        """Convert parameters to DeltaStream WITH clause."""
        params = {"url": self.url}
        if self.auth_type:
            params["auth.type"] = self.auth_type
        if self.username:
            params["username"] = self.username
        if self.password:
            params["password"] = self.password
        return WithClause(parameters=params)


@dataclass
class SchemaRegistryUpdateParams:
    """Parameters for updating a schema registry."""

    url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    comment: Optional[str] = None

    def to_with_clause(self) -> WithClause:
        """Convert update parameters to WITH clause."""
        params = {}
        if self.url:
            params["url"] = self.url
        if self.username:
            params["username"] = self.username
        if self.password:
            params["password"] = self.password
        return WithClause(parameters=params)
