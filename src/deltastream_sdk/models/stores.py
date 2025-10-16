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
    region: Optional[str] = None  # For AWS services
    endpoint: Optional[str] = None

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
            elif field_lower == "region":
                mapped_data["region"] = value
            elif field_lower == "endpoint":
                mapped_data["endpoint"] = value
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
    """
    Parameters for creating a data store.
    Use `additional_properties` for any custom or undocumented parameters.
    """

    name: str
    store_type: str  # Required: 'KAFKA', 'KINESIS', 'S3', 'SNOWFLAKE', 'DATABRICKS', 'POSTGRESQL', etc.

    # Common parameters (all store types)
    uris: Optional[str] = None  # List of comma-separated host:port URIs or endpoint URL
    tls_disabled: Optional[bool] = None  # Default: FALSE
    tls_verify_server_hostname: Optional[bool] = None  # Default: TRUE
    tls_ca_cert_file: Optional[str] = (
        None  # Path to CA certificate (prefix with @ to upload)
    )
    tls_cipher_suites: Optional[str] = None  # Comma-separated list of cipher suites
    tls_protocols: Optional[str] = None  # Comma-separated list of TLS protocols
    schema_registry_name: Optional[str] = None  # Name of associated schema registry
    properties_file: Optional[str] = (
        None  # Path to .yaml file with parameters (prefix with @)
    )

    # Kafka-specific parameters
    kafka_sasl_hash_function: Optional[str] = (
        None  # NONE, PLAIN, SHA256, SHA512, AWS_MSK_IAM
    )
    kafka_sasl_username: Optional[str] = (
        None  # Required if hash_function is PLAIN, SHA256, or SHA512
    )
    kafka_sasl_password: Optional[str] = (
        None  # Required if hash_function is PLAIN, SHA256, or SHA512
    )
    kafka_msk_aws_region: Optional[str] = (
        None  # Required if hash_function is AWS_MSK_IAM
    )
    kafka_msk_iam_role_arn: Optional[str] = (
        None  # Required if hash_function is AWS_MSK_IAM
    )
    tls_client_cert_file: Optional[str] = None  # For SHA256/SHA512 auth (prefix with @)
    tls_client_key_file: Optional[str] = None  # For SHA256/SHA512 auth (prefix with @)

    # Kinesis-specific parameters
    kinesis_iam_role_arn: Optional[str] = None  # For IAM role auth
    kinesis_access_key_id: Optional[str] = None  # For static credential auth
    kinesis_secret_access_key: Optional[str] = None  # For static credential auth

    # ClickHouse-specific parameters
    clickhouse_username: Optional[str] = None
    clickhouse_password: Optional[str] = None

    # Databricks-specific parameters
    databricks_app_token: Optional[str] = None  # Required
    databricks_warehouse_id: Optional[str] = None  # Required
    databricks_warehouse_port: Optional[int] = None  # Default: 443
    databricks_cloud_s3_bucket: Optional[str] = None  # Required
    databricks_cloud_region: Optional[str] = None  # Required (e.g., 'AWS us-east-1')

    # Iceberg REST Catalog-specific parameters
    iceberg_rest_client_id: Optional[str] = None
    iceberg_rest_client_secret: Optional[str] = None
    iceberg_rest_client_scope: Optional[str] = None
    iceberg_catalog_id: Optional[str] = None

    # PostgreSQL-specific parameters
    postgres_username: Optional[str] = None
    postgres_password: Optional[str] = None

    # Snowflake-specific parameters
    snowflake_account_id: Optional[str] = None  # Required
    snowflake_cloud_region: Optional[str] = None  # Required (e.g., 'AWS us-east-1')
    snowflake_role_name: Optional[str] = None  # Required
    snowflake_username: Optional[str] = None  # Required
    snowflake_warehouse_name: Optional[str] = None  # Required
    snowflake_client_key_file: Optional[str] = None  # Required (prefix with @)
    snowflake_client_key_passphrase: Optional[str] = None

    # S3 / AWS-specific parameters
    aws_iam_role_arn: Optional[str] = None
    aws_iam_external_id: Optional[str] = (
        None  # Required if aws_iam_role_arn is specified
    )
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: Optional[str] = None  # For Iceberg Glue

    # Iceberg-specific parameters
    iceberg_warehouse_default_path: Optional[str] = None  # For Glue catalog

    # Additional properties for custom/undocumented parameters
    additional_properties: Optional[Dict[str, str]] = None

    # Metadata
    comment: Optional[str] = None

    def to_with_clause(self) -> WithClause:
        """
        Convert parameters to DeltaStream WITH clause.
        """
        params = {}

        # Store type (required)
        if self.store_type:
            params["type"] = self.store_type

        # Common parameters
        if self.uris:
            params["uris"] = self.uris
        if self.tls_disabled is not None:
            params["tls.disabled"] = str(self.tls_disabled).upper()
        if self.tls_verify_server_hostname is not None:
            params["tls.verify_server_hostname"] = str(
                self.tls_verify_server_hostname
            ).upper()
        if self.tls_ca_cert_file:
            params["tls.ca_cert_file"] = self.tls_ca_cert_file
        if self.tls_cipher_suites:
            params["tls.cipher_suites"] = self.tls_cipher_suites
        if self.tls_protocols:
            params["tls.protocols"] = self.tls_protocols
        if self.schema_registry_name:
            params["schema_registry.name"] = self.schema_registry_name
        if self.properties_file:
            params["properties.file"] = self.properties_file

        # Kafka-specific parameters
        if self.kafka_sasl_hash_function:
            params["kafka.sasl.hash_function"] = self.kafka_sasl_hash_function
        if self.kafka_sasl_username:
            params["kafka.sasl.username"] = self.kafka_sasl_username
        if self.kafka_sasl_password:
            params["kafka.sasl.password"] = self.kafka_sasl_password
        if self.kafka_msk_aws_region:
            params["kafka.msk.aws_region"] = self.kafka_msk_aws_region
        if self.kafka_msk_iam_role_arn:
            params["kafka.msk.iam_role_arn"] = self.kafka_msk_iam_role_arn
        if self.tls_client_cert_file:
            params["tls.client.cert_file"] = self.tls_client_cert_file
        if self.tls_client_key_file:
            params["tls.client.key_file"] = self.tls_client_key_file

        # Kinesis-specific parameters
        if self.kinesis_iam_role_arn:
            params["kinesis.iam_role_arn"] = self.kinesis_iam_role_arn
        if self.kinesis_access_key_id:
            params["kinesis.access_key_id"] = self.kinesis_access_key_id
        if self.kinesis_secret_access_key:
            params["kinesis.secret_access_key"] = self.kinesis_secret_access_key

        # ClickHouse-specific parameters
        if self.clickhouse_username:
            params["clickhouse.username"] = self.clickhouse_username
        if self.clickhouse_password:
            params["clickhouse.password"] = self.clickhouse_password

        # Databricks-specific parameters
        if self.databricks_app_token:
            params["databricks.app_token"] = self.databricks_app_token
        if self.databricks_warehouse_id:
            params["databricks.warehouse_id"] = self.databricks_warehouse_id
        if self.databricks_warehouse_port is not None:
            params["databricks.warehouse_port"] = str(self.databricks_warehouse_port)
        if self.databricks_cloud_s3_bucket:
            params["databricks.cloud.s3.bucket"] = self.databricks_cloud_s3_bucket
        if self.databricks_cloud_region:
            params["databricks.cloud.region"] = self.databricks_cloud_region

        # Iceberg REST Catalog-specific parameters
        if self.iceberg_rest_client_id:
            params["iceberg.rest.client_id"] = self.iceberg_rest_client_id
        if self.iceberg_rest_client_secret:
            params["iceberg.rest.client_secret"] = self.iceberg_rest_client_secret
        if self.iceberg_rest_client_scope:
            params["iceberg.rest.client_scope"] = self.iceberg_rest_client_scope
        if self.iceberg_catalog_id:
            params["iceberg.catalog.id"] = self.iceberg_catalog_id

        # PostgreSQL-specific parameters
        if self.postgres_username:
            params["postgres.username"] = self.postgres_username
        if self.postgres_password:
            params["postgres.password"] = self.postgres_password

        # Snowflake-specific parameters
        if self.snowflake_account_id:
            params["snowflake.account_id"] = self.snowflake_account_id
        if self.snowflake_cloud_region:
            params["snowflake.cloud.region"] = self.snowflake_cloud_region
        if self.snowflake_role_name:
            params["snowflake.role_name"] = self.snowflake_role_name
        if self.snowflake_username:
            params["snowflake.username"] = self.snowflake_username
        if self.snowflake_warehouse_name:
            params["snowflake.warehouse_name"] = self.snowflake_warehouse_name
        if self.snowflake_client_key_file:
            params["snowflake.client.key_file"] = self.snowflake_client_key_file
        if self.snowflake_client_key_passphrase:
            params["snowflake.client.key_passphrase"] = (
                self.snowflake_client_key_passphrase
            )

        # S3 / AWS-specific parameters
        if self.aws_iam_role_arn:
            params["aws.iam_role_arn"] = self.aws_iam_role_arn
        if self.aws_iam_external_id:
            params["aws.iam_external_id"] = self.aws_iam_external_id
        if self.aws_access_key_id:
            params["aws.access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            params["aws.secret_access_key"] = self.aws_secret_access_key
        if self.aws_region:
            params["aws.region"] = self.aws_region

        # Iceberg-specific parameters
        if self.iceberg_warehouse_default_path:
            params["iceberg.warehouse.default_path"] = (
                self.iceberg_warehouse_default_path
            )

        # Additional properties (these override any defaults)
        if self.additional_properties:
            params.update(self.additional_properties)

        return WithClause(parameters=params)


@dataclass
class StoreUpdateParams:
    """Parameters for updating a data store."""

    # Additional properties for updates
    additional_properties: Optional[Dict[str, str]] = None

    # Metadata
    comment: Optional[str] = None

    def to_with_clause(self) -> WithClause:
        """Convert update parameters to WITH clause."""
        params = {}

        if self.additional_properties:
            params.update(self.additional_properties)

        return WithClause(parameters=params)
