"""
Tests for SDK models.
"""

from datetime import datetime

from deltastream_sdk.models import (
    Stream,
    Store,
    Database,
    ComputePool,
    StreamCreateParams,
    StoreCreateParams,
    ComputePoolCreateParams,
    WithClause,
)


class TestBaseModel:
    """Test BaseModel functionality."""

    def test_parse_datetime_iso_string(self):
        """Test parsing ISO datetime strings."""
        stream_data = {
            "Name": "test_stream",
            "CreatedAt": "2024-01-01T12:00:00Z",
            "UpdatedAt": "2024-01-02T12:00:00.123Z",
        }

        stream = Stream.from_dict(stream_data)

        assert isinstance(stream.created_at, datetime)
        assert stream.created_at.year == 2024
        assert stream.created_at.month == 1
        assert stream.created_at.day == 1

        assert isinstance(stream.updated_at, datetime)
        assert stream.updated_at.microsecond > 0  # Should parse milliseconds

    def test_parse_datetime_timestamp(self):
        """Test parsing timestamp numbers."""
        stream_data = {
            "Name": "test_stream",
            "CreatedAt": 1704110400,  # Unix timestamp for 2024-01-01 12:00:00
            "UpdatedAt": 1704196800,  # Unix timestamp for 2024-01-02 12:00:00
        }

        stream = Stream.from_dict(stream_data)

        assert isinstance(stream.created_at, datetime)
        assert stream.created_at.year == 2024
        assert stream.created_at.month == 1
        assert stream.created_at.day == 1

    def test_parse_datetime_none(self):
        """Test handling None datetime values."""
        stream_data = {"Name": "test_stream", "CreatedAt": None, "UpdatedAt": None}

        stream = Stream.from_dict(stream_data)

        assert stream.created_at is None
        assert stream.updated_at is None

    def test_to_dict(self):
        """Test converting model to dictionary."""
        stream = Stream(Name="test_stream", Owner="test_user")

        data = stream.to_dict()

        assert data["Name"] == "test_stream"
        assert data["Owner"] == "test_user"
        # Only fields that were provided are in the dict
        # CreatedAt and UpdatedAt are not in the dict if not provided

    def test_to_dict_with_all_fields(self):
        """Test converting model with all fields to dictionary."""
        stream = Stream(
            data={
                "Name": "test_stream",
                "Owner": "test_user",
                "CreatedAt": "2024-01-01T12:00:00Z",
                "UpdatedAt": "2024-01-02T12:00:00Z",
            }
        )

        data = stream.to_dict()

        assert data["Name"] == "test_stream"
        assert data["Owner"] == "test_user"
        assert "CreatedAt" in data
        assert "UpdatedAt" in data


class TestStreamModel:
    """Test Stream model."""

    def test_from_dict_complete(self, sample_stream_data):
        """Test creating Stream from complete data."""
        stream = Stream.from_dict(sample_stream_data)

        assert stream.name == "test_stream"
        assert stream.owner == "test_user"
        assert stream.stream_type == "STREAM"
        assert stream.state == "RUNNING"

    def test_from_dict_minimal(self):
        """Test creating Stream from minimal data."""
        minimal_data = {"Name": "minimal_stream"}

        stream = Stream.from_dict(minimal_data)

        assert stream.name == "minimal_stream"
        assert stream.owner is None

    def test_from_dict_with_unknown_fields(self):
        """Test creating Stream ignores unknown fields."""
        data_with_extras = {
            "Name": "test_stream",
            "unknown_field": "should_be_ignored",
            "another_unknown": 123,
        }

        stream = Stream.from_dict(data_with_extras)

        assert stream.name == "test_stream"
        assert not hasattr(stream, "unknown_field")
        assert not hasattr(stream, "another_unknown")


class TestStoreModel:
    """Test Store model."""

    def test_from_dict_complete(self, sample_store_data):
        """Test creating Store from complete data."""
        store = Store.from_dict(sample_store_data)

        assert store.name == "test_store"
        assert store.owner == "test_user"
        assert store.get("Type") == "KAFKA"
        assert store.get("State") == "ready"

        # Verify base fields are not in parameters
        assert "Owner" not in store.parameters
        assert "CreatedAt" not in store.parameters
        assert "UpdatedAt" not in store.parameters

    def test_from_dict_with_datetime_parsing(self):
        """Test that datetime fields are properly parsed."""
        from datetime import datetime

        data = {
            "Name": "datetime_store",
            "Owner": "admin",
            "CreatedAt": "2024-01-01T10:00:00Z",
            "UpdatedAt": "2024-01-02T15:30:00Z",
            "Type": "KAFKA",
        }

        store = Store.from_dict(data)

        assert store.name == "datetime_store"
        assert store.owner == "admin"
        assert isinstance(store.created_at, datetime)
        assert isinstance(store.updated_at, datetime)
        assert store.created_at.year == 2024
        assert store.created_at.month == 1
        assert store.created_at.day == 1

    def test_from_dict_minimal(self):
        """Test creating Store from minimal data."""
        minimal_data = {"Name": "minimal_store"}

        store = Store.from_dict(minimal_data)

        assert store.name == "minimal_store"
        assert store.owner is None
        assert store.created_at is None
        assert store.updated_at is None
        assert store.parameters.get("store_type") is None


class TestDatabaseModel:
    """Test Database model."""

    def test_from_dict_complete(self, sample_database_data):
        """Test creating Database from complete data."""
        database = Database.from_dict(sample_database_data)

        assert database.name == "test_database"
        assert database.owner == "test_user"
        assert database.is_default is False

    def test_from_dict_minimal(self):
        """Test creating Database from minimal data."""
        minimal_data = {"Name": "minimal_db"}

        database = Database.from_dict(minimal_data)

        assert database.name == "minimal_db"
        assert database.owner is None


class TestComputePoolModel:
    """Test ComputePool model."""

    def test_from_dict_complete(self, sample_compute_pool_data):
        """Test creating ComputePool from complete data."""
        pool = ComputePool.from_dict(sample_compute_pool_data)

        assert pool.name == "test_pool"
        assert pool.owner == "test_user"
        assert pool.size == "MEDIUM"
        assert pool.intended_state == "running"
        assert pool.actual_state == "running"

    def test_from_dict_with_string_numbers(self):
        """Test creating ComputePool with different states."""
        data = {
            "Name": "test_pool",
            "IntendedState": "stopped",
            "ActualState": "stopped",
            "Size": "LARGE",
            "Timeout": 600,
        }

        pool = ComputePool.from_dict(data)

        assert pool.intended_state == "stopped"
        assert pool.actual_state == "stopped"
        assert pool.size == "LARGE"
        assert pool.timeout == 600


class TestWithClause:
    """Test WithClause utility."""

    def test_to_sql_empty(self):
        """Test generating SQL from empty WITH clause."""
        with_clause = WithClause({})

        result = with_clause.to_sql()

        assert result == ""

    def test_to_sql_single_param(self):
        """Test generating SQL from single parameter."""
        with_clause = WithClause({"key": "value"})

        result = with_clause.to_sql()

        assert result == "WITH ('key' = 'value')"

    def test_to_sql_multiple_params(self):
        """Test generating SQL from multiple parameters."""
        with_clause = WithClause(
            {"bootstrap.servers": "localhost:9092", "auth.type": "PLAIN", "port": 9092}
        )

        result = with_clause.to_sql()

        # Should contain all parameters
        assert "WITH (" in result
        assert "'bootstrap.servers' = 'localhost:9092'" in result
        assert "'auth.type' = 'PLAIN'" in result
        assert "'port' = '9092'" in result

    def test_to_sql_special_characters(self):
        """Test WITH clause with special characters."""
        with_clause = WithClause(
            {
                "password": "pass'word",  # Contains single quote
                "topic": "test-topic_123",  # Contains special chars
            }
        )

        result = with_clause.to_sql()

        # Single quotes should be escaped
        assert "'password' = 'pass''word'" in result
        assert "'topic' = 'test-topic_123'" in result

    def test_from_dict(self):
        """Test creating WithClause from dictionary."""
        data = {"server": "localhost", "port": 9092, "ssl": True}

        with_clause = WithClause.from_dict(data)

        assert with_clause.parameters == data


class TestCreateParamsModels:
    """Test create parameters models."""

    def test_stream_create_params_to_with_clause(self):
        """Test StreamCreateParams to WITH clause conversion."""
        params = StreamCreateParams(
            name="test_stream",
            store="kafka_store",
            topic="test_topic",
            value_format="JSON",
            key_format="STRING",
        )

        with_clause = params.to_with_clause()
        sql = with_clause.to_sql()

        assert "'store' = 'kafka_store'" in sql
        assert "'topic' = 'test_topic'" in sql
        assert "'value.format' = 'JSON'" in sql
        assert "'key.format' = 'STRING'" in sql

    def test_store_create_params_to_with_clause(self):
        """Test StoreCreateParams to WITH clause conversion."""
        params = StoreCreateParams(
            name="test_store",
            type="KAFKA",
            parameters={
                "uris": "localhost:9092",
                "kafka.sasl.hash_function": "PLAIN",
                "kafka.sasl.username": "user",
                "kafka.sasl.password": "pass",
            },
        )

        with_clause = params.to_with_clause()
        sql = with_clause.to_sql()

        # Note: 'type' and 'kafka.sasl.hash_function' values are NOT quoted (SQL keywords)
        assert "'type' = KAFKA" in sql
        assert "'uris' = 'localhost:9092'" in sql
        assert "'kafka.sasl.hash_function' = PLAIN" in sql
        assert "'kafka.sasl.username' = 'user'" in sql
        assert "'kafka.sasl.password' = 'pass'" in sql

    def test_store_create_params_with_additional_properties(self):
        """Test StoreCreateParams with properties dict."""
        params = StoreCreateParams(
            name="test_store",
            type="KAFKA",
            parameters={
                "uris": "localhost:9092",
                "custom.param": "custom_value",
                "another.param": "another_value",
            },
        )

        with_clause = params.to_with_clause()
        sql = with_clause.to_sql()

        assert "'type' = KAFKA" in sql
        assert "'uris' = 'localhost:9092'" in sql
        assert "'custom.param' = 'custom_value'" in sql
        assert "'another.param' = 'another_value'" in sql

    def test_store_create_params_kinesis(self):
        """Test StoreCreateParams for Kinesis with IAM role."""
        params = StoreCreateParams(
            name="kinesis_store",
            type="KINESIS",
            parameters={
                "uris": "https://url.to.kinesis.aws:4566",
                "kinesis.iam_role_arn": "arn:aws:iam::123456789012:role/example-IAM-role",
            },
        )

        with_clause = params.to_with_clause()
        sql = with_clause.to_sql()

        assert "'type' = KINESIS" in sql
        assert "'uris' = 'https://url.to.kinesis.aws:4566'" in sql
        assert (
            "'kinesis.iam_role_arn' = 'arn:aws:iam::123456789012:role/example-IAM-role'"
            in sql
        )

    def test_store_create_params_snowflake(self):
        """Test StoreCreateParams for Snowflake."""
        params = StoreCreateParams(
            name="snowflake_store",
            type="SNOWFLAKE",
            parameters={
                "uris": "https://my-account.snowflakecomputing.com",
                "snowflake.account_id": "my-account",
                "snowflake.role_name": "ACCOUNTADMIN",
                "snowflake.username": "STREAMING_USER",
                "snowflake.warehouse_name": "COMPUTE_WH",
                "snowflake.client.key_file": "@/path/to/pk/my_account_rsa.p8",
            },
        )

        with_clause = params.to_with_clause()
        sql = with_clause.to_sql()

        assert "'type' = SNOWFLAKE" in sql
        assert "'uris' = 'https://my-account.snowflakecomputing.com'" in sql
        assert "'snowflake.account_id' = 'my-account'" in sql
        assert "'snowflake.role_name' = 'ACCOUNTADMIN'" in sql
        assert "'snowflake.username' = 'STREAMING_USER'" in sql
        assert "'snowflake.warehouse_name' = 'COMPUTE_WH'" in sql
        assert "'snowflake.client.key_file' = '@/path/to/pk/my_account_rsa.p8'" in sql

    def test_compute_pool_create_params_to_with_clause(self):
        """Test ComputePoolCreateParams to WITH clause conversion."""
        params = ComputePoolCreateParams(
            name="test_pool",
            size="MEDIUM",
            min_units=1,
            max_units=5,
            auto_suspend=True,
            auto_suspend_minutes=15,
        )

        with_clause = params.to_with_clause()
        sql = with_clause.to_sql()

        assert "'size' = 'MEDIUM'" in sql
        assert "'min.units' = '1'" in sql
        assert "'max.units' = '5'" in sql
        assert "'auto.suspend' = 'true'" in sql
        assert "'auto.suspend.minutes' = '15'" in sql

    def test_create_params_exclude_none_values(self):
        """Test that None values are excluded from WITH clause."""
        params = StreamCreateParams(
            name="test_stream",
            store="kafka_store",
            topic="test_topic",
            value_format=None,  # Should be excluded
            key_format="STRING",
        )

        with_clause = params.to_with_clause()
        sql = with_clause.to_sql()

        assert "'store' = 'kafka_store'" in sql
        assert "'topic' = 'test_topic'" in sql
        assert "'key.format' = 'STRING'" in sql
        assert "value.format" not in sql  # Should be excluded
