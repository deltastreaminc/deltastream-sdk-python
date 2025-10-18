"""Schema Registry models for DeltaStream SDK."""

from dataclasses import dataclass
from typing import Optional
from .base import BaseModel, WithClause


class SchemaRegistry(BaseModel):
    """Model representing a DeltaStream schema registry."""

    @property
    def registry_type(self) -> Optional[str]:
        """Get the registry type."""
        return self._data.get("Type")

    @property
    def url(self) -> Optional[str]:
        """Get the URL."""
        return self._data.get("Url")


@dataclass
class SchemaRegistryCreateParams:
    """Parameters for creating a schema registry."""

    name: str
    url: str
    auth_type: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    def to_with_clause(self) -> WithClause:
        """Convert parameters to DeltaStream WITH clause."""
        parameters = {"url": self.url}
        if self.auth_type:
            parameters["auth.type"] = self.auth_type
        if self.username:
            parameters["username"] = self.username
        if self.password:
            parameters["password"] = self.password
        return WithClause(parameters=parameters)


@dataclass
class SchemaRegistryUpdateParams:
    """Parameters for updating a schema registry."""

    url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    def to_with_clause(self) -> WithClause:
        """Convert update parameters to WITH clause."""
        parameters = {}
        if self.url:
            parameters["url"] = self.url
        if self.username:
            parameters["username"] = self.username
        if self.password:
            parameters["password"] = self.password
        return WithClause(parameters=parameters)
