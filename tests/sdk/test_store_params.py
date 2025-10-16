"""
Test script to validate StoreCreateParams for all documented store types.
"""

from deltastream_sdk.models.stores import StoreCreateParams


def test_kafka_store():
    """Test Kafka store parameters."""
    params = StoreCreateParams(
        name="kafka_store",
        store_type="KAFKA",
        uris="kafka:9092",
        kafka_sasl_hash_function="PLAIN",
        kafka_sasl_username="user",
        kafka_sasl_password="pass",
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = KAFKA" in sql
    assert "'kafka.sasl.hash_function' = PLAIN" in sql
    print("✅ Kafka store test passed")


def test_kinesis_store():
    """Test Kinesis store parameters."""
    params = StoreCreateParams(
        name="kinesis_store",
        store_type="KINESIS",
        uris="https://kinesis.amazonaws.com",
        kinesis_iam_role_arn="arn:aws:iam::123456789012:role/my-role",
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = KINESIS" in sql
    assert "'kinesis.iam_role_arn'" in sql
    print("✅ Kinesis store test passed")


def test_snowflake_store():
    """Test Snowflake store parameters."""
    params = StoreCreateParams(
        name="snowflake_store",
        store_type="SNOWFLAKE",
        uris="https://account.snowflakecomputing.com",
        snowflake_account_id="my-account",
        snowflake_role_name="ACCOUNTADMIN",
        snowflake_username="user",
        snowflake_warehouse_name="WAREHOUSE",
        snowflake_client_key_file="@/path/to/key.pem",
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = SNOWFLAKE" in sql
    assert "'snowflake.account_id'" in sql
    assert "'snowflake.client.key_file'" in sql
    print("✅ Snowflake store test passed")


def test_databricks_store():
    """Test Databricks store parameters."""
    params = StoreCreateParams(
        name="databricks_store",
        store_type="DATABRICKS",
        uris="https://dbc-123.cloud.databricks.com",
        databricks_app_token="token",
        databricks_warehouse_id="warehouse-id",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        databricks_cloud_s3_bucket="bucket",
        databricks_cloud_region="AWS us-west-2",
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = DATABRICKS" in sql
    assert "'databricks.app_token'" in sql
    assert "'databricks.cloud.s3.bucket'" in sql
    print("✅ Databricks store test passed")


def test_postgresql_store():
    """Test PostgreSQL store parameters."""
    params = StoreCreateParams(
        name="postgres_store",
        store_type="POSTGRESQL",
        uris="postgresql://host:5432/db",
        postgres_username="user",
        postgres_password="pass",
        tls_verify_server_hostname=True,
        tls_disabled=False,
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = POSTGRESQL" in sql
    assert "'postgres.username'" in sql
    assert "'tls.verify_server_hostname' = TRUE" in sql
    print("✅ PostgreSQL store test passed")


def test_clickhouse_store():
    """Test ClickHouse store parameters."""
    params = StoreCreateParams(
        name="clickhouse_store",
        store_type="CLICKHOUSE",
        uris="jdbc:clickhouse://host:8443",
        clickhouse_username="user",
        clickhouse_password="pass",
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = CLICKHOUSE" in sql
    assert "'clickhouse.username'" in sql
    print("✅ ClickHouse store test passed")


def test_s3_store():
    """Test S3 store parameters."""
    params = StoreCreateParams(
        name="s3_store",
        store_type="S3",
        uris="https://bucket.s3.amazonaws.com/",
        aws_iam_role_arn="arn:aws:iam::123456789012:role/my-role",
        aws_iam_external_id="external-id",
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = S3" in sql
    assert "'aws.iam_role_arn'" in sql
    assert "'aws.iam_external_id'" in sql
    print("✅ S3 store test passed")


def test_iceberg_rest_store():
    """Test Iceberg REST Catalog store parameters."""
    params = StoreCreateParams(
        name="iceberg_rest_store",
        store_type="ICEBERG_REST",
        uris="https://catalog.com/api",
        iceberg_catalog_id="catalog-id",
        iceberg_rest_client_id="client-id",
        iceberg_rest_client_secret="secret",
        iceberg_rest_client_scope="scope",
    )
    sql = params.to_with_clause().to_sql()
    assert "'type' = ICEBERG_REST" in sql
    assert "'iceberg.catalog.id'" in sql
    assert "'iceberg.rest.client_id'" in sql
    print("✅ Iceberg REST store test passed")


def test_additional_properties():
    """Test that additional_properties works correctly."""
    params = StoreCreateParams(
        name="custom_store",
        store_type="KAFKA",
        uris="kafka:9092",
        additional_properties={
            "custom.parameter": "custom_value",
            "override.something": "override_value",
        },
    )
    sql = params.to_with_clause().to_sql()
    assert "'custom.parameter' = 'custom_value'" in sql
    assert "'override.something' = 'override_value'" in sql
    print("✅ Additional properties test passed")


def test_tls_parameters():
    """Test TLS configuration parameters."""
    params = StoreCreateParams(
        name="secure_store",
        store_type="KAFKA",
        uris="kafka:9092",
        tls_disabled=False,
        tls_verify_server_hostname=True,
        tls_ca_cert_file="@/path/to/ca.pem",
        tls_cipher_suites="TLS_AES_256_GCM_SHA384",
        tls_protocols="TLSv1.2,TLSv1.3",
    )
    sql = params.to_with_clause().to_sql()
    assert "'tls.disabled' = FALSE" in sql
    assert "'tls.verify_server_hostname' = TRUE" in sql
    assert "'tls.ca_cert_file'" in sql
    assert "'tls.cipher_suites'" in sql
    assert "'tls.protocols'" in sql
    print("✅ TLS parameters test passed")


if __name__ == "__main__":
    print("Testing StoreCreateParams with documented parameters...\n")

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

    print("\n🎉 All store type tests passed!")
    print("\nAll parameters match DeltaStream CREATE STORE documentation.")
