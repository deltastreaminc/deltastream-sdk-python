"""
Store resource manager for DeltaStream SDK.
"""

from typing import Optional, List, Dict, Any
from .base import BaseResourceManager
from ..models.stores import Store, StoreCreateParams, StoreUpdateParams


class StoreManager(BaseResourceManager[Store]):
    """Manager for DeltaStream data store resources."""

    def __init__(self, connection):
        super().__init__(connection, Store)

    def _get_list_sql(self, **filters) -> str:
        """Generate SQL for listing stores."""
        sql = "LIST STORES"

        # Add filters if provided
        where_clauses = []
        if filters.get("type"):
            where_clauses.append(f"type = '{filters['type']}'")

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        return sql

    def _get_describe_sql(self, name: str) -> str:
        """Generate SQL for describing a specific store."""
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE STORE {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        """Generate SQL for creating a store."""
        if isinstance(params.get("params"), StoreCreateParams):
            create_params = params["params"]
        else:
            # Convert dict params to StoreCreateParams
            create_params = StoreCreateParams(**params)

        name = self._escape_identifier(create_params.name)

        # Build CREATE STORE statement
        sql = f"CREATE STORE {name}"

        # Add WITH clause for connection parameters
        with_clause = create_params.to_with_clause()
        if with_clause.parameters:
            sql += f" {with_clause.to_sql()}"

        return sql

    def _get_update_sql(self, name: str, **params) -> str:
        """Generate SQL for updating a store."""
        escaped_name = self._escape_identifier(name)

        if isinstance(params.get("params"), StoreUpdateParams):
            update_params = params["params"]
        else:
            update_params = StoreUpdateParams(**params)

        # Build UPDATE STORE statement
        sql = f"UPDATE STORE {escaped_name}"

        # Add WITH clause for updated parameters
        with_clause = update_params.to_with_clause()
        if with_clause.parameters:
            sql += f" {with_clause.to_sql()}"

        return sql

    def _get_delete_sql(self, name: str, **params) -> str:
        """Generate SQL for deleting a store."""
        escaped_name = self._escape_identifier(name)
        return f"DROP STORE {escaped_name}"

    # Store-specific operations
    async def create_kafka_store(
        self,
        name: str,
        uris: str,
        kafka_sasl_hash_function: Optional[str] = None,
        kafka_sasl_username: Optional[str] = None,
        kafka_sasl_password: Optional[str] = None,
        schema_registry_name: Optional[str] = None,
        **kwargs,
    ) -> Store:
        """
        Create a Kafka data store.

        Args:
            name: Name of the store
            uris: Comma-separated list of broker URIs (e.g., 'kafka:9092,kafka2:9092')
            kafka_sasl_hash_function: SASL hash function (NONE, PLAIN, SHA256, SHA512, AWS_MSK_IAM)
            kafka_sasl_username: Username for SASL authentication
            kafka_sasl_password: Password for SASL authentication
            schema_registry_name: Name of associated schema registry
            **kwargs: Additional properties (e.g., tls_ca_cert_file, kafka_msk_aws_region)

        Returns:
            Created Store object
        """
        params = StoreCreateParams(
            name=name,
            store_type="KAFKA",
            uris=uris,
            kafka_sasl_hash_function=kafka_sasl_hash_function,
            kafka_sasl_username=kafka_sasl_username,
            kafka_sasl_password=kafka_sasl_password,
            schema_registry_name=schema_registry_name,
            **kwargs,
        )
        return await self.create(params=params)

    async def create_kinesis_store(
        self,
        name: str,
        uris: str,
        kinesis_access_key_id: Optional[str] = None,
        kinesis_secret_access_key: Optional[str] = None,
        kinesis_iam_role_arn: Optional[str] = None,
        **kwargs,
    ) -> Store:
        """
        Create a Kinesis data store.

        Args:
            name: Name of the store
            uris: Kinesis endpoint URI (e.g., 'https://kinesis.us-east-1.amazonaws.com')
            kinesis_access_key_id: AWS access key for static credentials
            kinesis_secret_access_key: AWS secret key for static credentials
            kinesis_iam_role_arn: IAM role ARN for authentication
            **kwargs: Additional properties

        Returns:
            Created Store object
        """
        params = StoreCreateParams(
            name=name,
            store_type="KINESIS",
            uris=uris,
            kinesis_access_key_id=kinesis_access_key_id,
            kinesis_secret_access_key=kinesis_secret_access_key,
            kinesis_iam_role_arn=kinesis_iam_role_arn,
            **kwargs,
        )
        return await self.create(params=params)

    async def create_s3_store(
        self,
        name: str,
        uris: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_iam_role_arn: Optional[str] = None,
        aws_iam_external_id: Optional[str] = None,
        **kwargs,
    ) -> Store:
        """
        Create an S3 data store.

        Args:
            name: Name of the store
            uris: S3 bucket URI (e.g., 'https://mybucket.s3.amazonaws.com/')
            aws_access_key_id: AWS access key for static credentials
            aws_secret_access_key: AWS secret key for static credentials
            aws_iam_role_arn: IAM role ARN for authentication
            aws_iam_external_id: External ID for IAM role assumption
            **kwargs: Additional properties

        Returns:
            Created Store object
        """
        params = StoreCreateParams(
            name=name,
            store_type="S3",
            uris=uris,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_iam_role_arn=aws_iam_role_arn,
            aws_iam_external_id=aws_iam_external_id,
            **kwargs,
        )
        return await self.create(params=params)

    async def test_connection(self, name: str) -> Dict[str, Any]:
        """Test the connection to a data store."""
        escaped_name = self._escape_identifier(name)
        sql = f"TEST STORE {escaped_name}"
        results = await self._query_sql(sql)
        return results[0] if results else {"status": "unknown"}

    async def get_topics(self, name: str) -> List[str]:
        """Get list of topics/streams available in the store."""
        escaped_name = self._escape_identifier(name)
        sql = f"LIST TOPICS FROM STORE {escaped_name}"
        results = await self._query_sql(sql)
        return [result.get("topic_name", result.get("name", "")) for result in results]
