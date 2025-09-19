"""
Store models for DeltaStream SDK.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from .base import BaseModel, WithClause


@dataclass
class Store(BaseModel):
    """Model representing a DeltaStream data store."""

    # Core store properties
    store_type: Optional[str] = None  # 'KAFKA', 'KINESIS', 'S3', etc.
    status: Optional[str] = None
    is_default: Optional[bool] = None

    # Connection properties
    bootstrap_servers: Optional[str] = None  # For Kafka
    region: Optional[str] = None  # For AWS services
    endpoint: Optional[str] = None

    # Authentication
    auth_type: Optional[str] = None
    username: Optional[str] = None
    # Note: passwords/secrets not stored in model for security

    # Schema registry (for Kafka/Confluent)
    schema_registry_url: Optional[str] = None
    schema_registry_auth: Optional[str] = None

    # SSL/TLS configuration
    ssl_enabled: Optional[bool] = None
    ssl_ca_location: Optional[str] = None

    # Additional metadata
    database_name: Optional[str] = None
    schema_name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Store":
        """Create Store instance from dictionary."""
        mapped_data = {}

        # Use parent class mapping for common fields first
        base_data = super().from_dict(data).to_dict()
        mapped_data.update(base_data)

        # Apply child class specific mappings (will override base mappings if there are conflicts)
        for field_name, value in data.items():
            field_lower = field_name.lower()

            if field_lower in ("type", "store_type"):
                mapped_data["store_type"] = value
            elif field_lower in ("status", "state"):
                mapped_data["status"] = value
            elif field_lower in ("is_default", "default", "is default"):
                mapped_data["is_default"] = value
            elif field_lower in ("bootstrap_servers", "bootstrap.servers"):
                mapped_data["bootstrap_servers"] = value
            elif field_lower == "region":
                mapped_data["region"] = value
            elif field_lower == "endpoint":
                mapped_data["endpoint"] = value
            elif field_lower in ("auth_type", "authentication_type"):
                mapped_data["auth_type"] = value
            elif field_lower == "username":
                mapped_data["username"] = value
            elif field_lower in ("schema_registry_url", "schema.registry.url"):
                mapped_data["schema_registry_url"] = value
            elif field_lower in ("schema_registry_auth", "schema.registry.auth"):
                mapped_data["schema_registry_auth"] = value
            elif field_lower in ("ssl_enabled", "ssl.enabled"):
                mapped_data["ssl_enabled"] = value
            elif field_lower in ("ssl_ca_location", "ssl.ca.location"):
                mapped_data["ssl_ca_location"] = value
            elif field_lower in ("database", "database_name"):
                mapped_data["database_name"] = value
            elif field_lower in ("schema", "schema_name"):
                mapped_data["schema_name"] = value

        # Filter to only include fields that exist in this dataclass
        field_names = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in mapped_data.items() if k in field_names}

        return cls(**filtered_data)


@dataclass
class StoreCreateParams:
    """Parameters for creating a data store."""

    name: str
    store_type: str  # Required: 'KAFKA', 'KINESIS', 'S3', etc.

    # Connection configuration
    bootstrap_servers: Optional[str] = None  # For Kafka
    region: Optional[str] = None  # For AWS services
    endpoint: Optional[str] = None

    # Authentication
    auth_type: Optional[str] = None  # 'PLAIN', 'SASL_PLAINTEXT', 'IAM', etc.
    username: Optional[str] = None
    password: Optional[str] = None

    # SSL/TLS configuration
    ssl_enabled: Optional[bool] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None

    # Schema registry (for Kafka/Confluent)
    schema_registry_url: Optional[str] = None
    schema_registry_username: Optional[str] = None
    schema_registry_password: Optional[str] = None

    # AWS-specific settings
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None

    # Additional properties
    additional_properties: Optional[Dict[str, str]] = None

    # Metadata
    comment: Optional[str] = None

    def to_with_clause(self) -> WithClause:
        """Convert parameters to DeltaStream WITH clause."""
        params = {}

        # Store type
        if self.store_type:
            params["type"] = self.store_type

        # Core connection parameters
        if self.bootstrap_servers:
            params["bootstrap.servers"] = self.bootstrap_servers
        if self.region:
            params["region"] = self.region
        if self.endpoint:
            params["endpoint"] = self.endpoint

        # Authentication
        if self.auth_type:
            params["auth.type"] = self.auth_type
        if self.username:
            params["auth.username"] = self.username
        if self.password:
            params["auth.password"] = self.password

        # SSL/TLS
        if self.ssl_enabled is not None:
            params["ssl.enabled"] = str(self.ssl_enabled).lower()
        if self.ssl_ca_location:
            params["ssl.ca.location"] = self.ssl_ca_location
        if self.ssl_certificate_location:
            params["ssl.certificate.location"] = self.ssl_certificate_location
        if self.ssl_key_location:
            params["ssl.key.location"] = self.ssl_key_location

        # Schema registry
        if self.schema_registry_url:
            params["schema.registry.url"] = self.schema_registry_url
        if self.schema_registry_username:
            params["schema.registry.username"] = self.schema_registry_username
        if self.schema_registry_password:
            params["schema.registry.password"] = self.schema_registry_password

        # AWS credentials
        if self.access_key_id:
            params["aws.access.key.id"] = self.access_key_id
        if self.secret_access_key:
            params["aws.secret.access.key"] = self.secret_access_key
        if self.session_token:
            params["aws.session.token"] = self.session_token

        # Additional properties
        if self.additional_properties:
            params.update(self.additional_properties)

        return WithClause(parameters=params)


@dataclass
class StoreUpdateParams:
    """Parameters for updating a data store."""

    # Connection configuration updates
    bootstrap_servers: Optional[str] = None
    region: Optional[str] = None
    endpoint: Optional[str] = None

    # Authentication updates
    username: Optional[str] = None
    password: Optional[str] = None

    # Schema registry updates
    schema_registry_url: Optional[str] = None
    schema_registry_username: Optional[str] = None
    schema_registry_password: Optional[str] = None

    # Additional properties
    additional_properties: Optional[Dict[str, str]] = None

    # Metadata
    comment: Optional[str] = None

    def to_with_clause(self) -> WithClause:
        """Convert update parameters to WITH clause."""
        params = {}

        if self.bootstrap_servers:
            params["bootstrap.servers"] = self.bootstrap_servers
        if self.region:
            params["region"] = self.region
        if self.endpoint:
            params["endpoint"] = self.endpoint
        if self.username:
            params["username"] = self.username
        if self.password:
            params["password"] = self.password
        if self.schema_registry_url:
            params["schema.registry.url"] = self.schema_registry_url
        if self.schema_registry_username:
            params["schema.registry.username"] = self.schema_registry_username
        if self.schema_registry_password:
            params["schema.registry.password"] = self.schema_registry_password

        if self.additional_properties:
            params.update(self.additional_properties)

        return WithClause(parameters=params)
