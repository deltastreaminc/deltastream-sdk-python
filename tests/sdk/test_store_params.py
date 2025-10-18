"""
Test script to validate StoreCreateParams for all documented store types.
"""

from deltastream_sdk.models.stores import StoreCreateParams


def test_kafka_store():
    """Test Kafka store parameters."""
    params = StoreCreateParams(
        name="kafka_store",
        type="KAFKA",
        parameters={
            "uris": "kafka:9092",
            "kafka.sasl.hash_function": "PLAIN",
            "kafka.sasl.username": "user",
            "kafka.sasl.password": "pass",
        },
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = KAFKA" in sql
    assert "'kafka.sasl.hash_function' = PLAIN" in sql


def test_kinesis_store():
    """Test Kinesis store parameters."""
    params = StoreCreateParams(
        name="kinesis_store",
        type="KINESIS",
        parameters={
            "uris": "https://kinesis.amazonaws.com",
            "kinesis.iam_role_arn": "arn:aws:iam::123456789012:role/my-role",
        },
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = KINESIS" in sql
    assert "'kinesis.iam_role_arn'" in sql


def test_snowflake_store():
    """Test Snowflake store parameters."""
    params = StoreCreateParams(
        name="snowflake_store",
        type="SNOWFLAKE",
        parameters={
            "uris": "https://account.snowflakecomputing.com",
            "snowflake.account_id": "my-account",
            "snowflake.role_name": "ACCOUNTADMIN",
            "snowflake.username": "user",
            "snowflake.warehouse_name": "WAREHOUSE",
            "snowflake.client.key_file": "@/path/to/key.pem",
        },
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = SNOWFLAKE" in sql
    assert "'snowflake.account_id'" in sql
    assert "'snowflake.client.key_file'" in sql


def test_databricks_store():
    """Test Databricks store parameters."""
    params = StoreCreateParams(
        name="databricks_store",
        type="DATABRICKS",
        parameters={
            "uris": "https://dbc-123.cloud.databricks.com",
            "databricks.app_token": "token",
            "databricks.warehouse_id": "warehouse-id",
            "aws.access_key_id": "key",
            "aws.secret_access_key": "secret",
            "databricks.cloud.s3.bucket": "bucket",
            "databricks.cloud.region": "AWS us-west-2",
        },
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = DATABRICKS" in sql
    assert "'databricks.app_token'" in sql
    assert "'databricks.cloud.s3.bucket'" in sql


def test_postgresql_store():
    """Test PostgreSQL store parameters."""
    params = StoreCreateParams(
        name="postgres_store",
        type="POSTGRESQL",
        parameters={
            "uris": "postgresql://host:5432/db",
            "postgres.username": "user",
            "postgres.password": "pass",
            "tls.verify_server_hostname": "TRUE",
            "tls.disabled": "FALSE",
        },
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = POSTGRESQL" in sql
    assert "'postgres.username'" in sql
    assert "'tls.verify_server_hostname' = TRUE" in sql


def test_clickhouse_store():
    """Test ClickHouse store parameters."""
    params = StoreCreateParams(
        name="clickhouse_store",
        type="CLICKHOUSE",
        parameters={
            "uris": "jdbc:clickhouse://host:8443",
            "clickhouse.username": "user",
            "clickhouse.password": "pass",
        },
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = CLICKHOUSE" in sql
    assert "'clickhouse.username'" in sql


def test_s3_store():
    """Test S3 store parameters."""
    params = StoreCreateParams(
        name="s3_store",
        type="S3",
        parameters={
            "uris": "https://bucket.s3.amazonaws.com/",
            "aws.iam_role_arn": "arn:aws:iam::123456789012:role/my-role",
            "aws.iam_external_id": "external-id",
        },
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = S3" in sql
    assert "'aws.iam_role_arn'" in sql
    assert "'aws.iam_external_id'" in sql


def test_iceberg_rest_store():
    """Test Iceberg REST Catalog store parameters."""
    params = StoreCreateParams(
        name="iceberg_rest_store",
        type="ICEBERG_REST",
        parameters={
            "uris": "https://catalog.com/api",
            "iceberg.catalog.id": "catalog-id",
            "iceberg.rest.client_id": "client-id",
            "iceberg.rest.client_secret": "secret",
            "iceberg.rest.client_scope": "scope",
        },
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = ICEBERG_REST" in sql
    assert "'iceberg.catalog.id'" in sql
    assert "'iceberg.rest.client_id'" in sql


def test_additional_properties():
    """Test that properties dict works correctly."""
    params = StoreCreateParams(
        name="custom_store",
        type="KAFKA",
        parameters={
            "uris": "kafka:9092",
            "custom.parameter": "custom_value",
            "override.something": "override_value",
        },
    )
    sql = params.to_with_clause().to_sql()
    assert "'custom.parameter' = 'custom_value'" in sql
    assert "'override.something' = 'override_value'" in sql


def test_tls_parameters():
    """Test TLS configuration parameters."""
    params = StoreCreateParams(
        name="secure_store",
        type="KAFKA",
        parameters={
            "uris": "kafka:9092",
            "tls.disabled": "FALSE",
            "tls.verify_server_hostname": "TRUE",
            "tls.ca_cert_file": "@/path/to/ca.pem",
            "tls.cipher_suites": "TLS_AES_256_GCM_SHA384",
            "tls.protocols": "TLSv1.2,TLSv1.3",
        },
    )
    sql = params.to_with_clause().to_sql()
    assert "'tls.disabled' = FALSE" in sql
    assert "'tls.verify_server_hostname' = TRUE" in sql
    assert "'tls.ca_cert_file'" in sql
    assert "'tls.cipher_suites'" in sql
    assert "'tls.protocols'" in sql


if __name__ == "__main__":
    test_kafka_store()
    test_kinesis_store()
    test_snowflake_store()
    test_databricks_store()
    test_postgresql_store()
    test_clickhouse_store()
    test_s3_store()
    test_iceberg_rest_store()
    test_additional_properties()
    test_tls_parameters()
