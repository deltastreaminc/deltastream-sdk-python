"""
Tests for SDK resource managers.
"""

import pytest
from unittest.mock import AsyncMock

from deltastream_sdk.resources import (
    StreamManager,
    StoreManager,
    DatabaseManager,
    ComputePoolManager,
)
from deltastream_sdk.models import Stream, Database, ComputePool
from deltastream_sdk.exceptions import ResourceNotFound


class TestBaseResourceManager:
    """Test BaseResourceManager functionality."""

    @pytest.mark.asyncio
    async def test_execute_sql(self, mock_connection):
        """Test _execute_sql method."""
        manager = StreamManager(mock_connection)

        await manager._execute_sql("CREATE STREAM test")

        mock_connection.exec.assert_called_once_with("CREATE STREAM test;")

    @pytest.mark.asyncio
    async def test_query_sql(self, mock_connection, mock_query_rows):
        """Test _query_sql method."""
        manager = StreamManager(mock_connection)
        mock_connection.query.return_value = mock_query_rows

        result = await manager._query_sql("LIST STREAMS")

        mock_connection.query.assert_called_once_with("LIST STREAMS;")
        assert result == [{"name": "test_stream"}, {"name": "another_stream"}]

    def test_escape_identifier(self, mock_connection):
        """Test SQL identifier escaping."""
        manager = StreamManager(mock_connection)

        # Normal identifier
        assert manager._escape_identifier("test_stream") == '"test_stream"'

        # Identifier with special characters
        assert manager._escape_identifier("test-stream") == '"test-stream"'

        # Identifier with quotes (should be escaped)
        assert manager._escape_identifier('test"stream') == '"test""stream"'

    def test_escape_string(self, mock_connection):
        """Test SQL string escaping."""
        manager = StreamManager(mock_connection)

        # Normal string
        assert manager._escape_string("test value") == "'test value'"

        # String with single quotes (should be escaped)
        assert manager._escape_string("test's value") == "'test''s value'"

        # Empty string
        assert manager._escape_string("") == "''"


class TestStreamManager:
    """Test StreamManager."""

    @pytest.mark.asyncio
    async def test_list_streams(self, mock_connection, mock_list_result):
        """Test listing streams."""
        manager = StreamManager(mock_connection)
        mock_connection.query.return_value = mock_list_result(["stream1", "stream2"])

        streams = await manager.list()

        mock_connection.query.assert_called_once_with("LIST STREAMS;")
        assert len(streams) == 2
        assert streams[0].name == "stream1"
        assert streams[1].name == "stream2"

    @pytest.mark.asyncio
    async def test_get_stream(
        self, mock_connection, mock_describe_result, sample_stream_data
    ):
        """Test getting a specific stream."""
        manager = StreamManager(mock_connection)
        mock_connection.query.return_value = mock_describe_result(
            "STREAM", sample_stream_data
        )

        stream = await manager.get("test_stream")

        expected_sql = 'DESCRIBE RELATION "test_stream";'
        mock_connection.query.assert_called_once_with(expected_sql)
        assert isinstance(stream, Stream)
        assert stream.name == "test_stream"

    @pytest.mark.asyncio
    async def test_get_stream_not_found(self, mock_connection):
        """Test getting non-existent stream raises exception."""
        manager = StreamManager(mock_connection)

        # Mock empty result
        mock_rows = AsyncMock()
        mock_rows.columns = lambda: []

        async def empty_iter(self):
            return
            yield  # This will never execute

        mock_rows.__aiter__ = empty_iter
        mock_connection.query.return_value = mock_rows

        with pytest.raises(ResourceNotFound):
            await manager.get("nonexistent_stream")

    @pytest.mark.asyncio
    async def test_create_stream_with_schema(
        self, mock_connection, mock_describe_result, sample_stream_data
    ):
        """Test creating stream with explicit schema."""
        manager = StreamManager(mock_connection)

        # Mock the query call for get() after creation
        mock_connection.query.return_value = mock_describe_result(
            "stream", sample_stream_data
        )

        await manager.create_with_schema(
            name="test_stream",
            columns=[
                {"name": "id", "type": "INTEGER"},
                {"name": "message", "type": "VARCHAR"},
            ],
            store="kafka_store",
            topic="test_topic",
            value_format="JSON",
        )

        # Verify SQL generation
        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE STREAM "test_stream"' in call_args
        assert '"id" INTEGER' in call_args
        assert '"message" VARCHAR' in call_args
        assert "'store' = 'kafka_store'" in call_args
        assert "'topic' = 'test_topic'" in call_args
        assert "'value.format' = 'JSON'" in call_args

    @pytest.mark.asyncio
    async def test_create_stream_from_select(
        self, mock_connection, mock_describe_result, sample_stream_data
    ):
        """Test creating stream from SELECT query."""
        manager = StreamManager(mock_connection)

        # Mock the query call for get() after creation
        derived_stream_data = sample_stream_data.copy()
        derived_stream_data["name"] = "derived_stream"
        mock_connection.query.return_value = mock_describe_result(
            "stream", derived_stream_data
        )

        await manager.create_from_select(
            name="derived_stream",
            sql_definition="SELECT * FROM source_stream",
            store="kafka_store",
            topic="derived_topic",
        )

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE STREAM "derived_stream"' in call_args
        assert "AS SELECT * FROM source_stream" in call_args
        assert "'store' = 'kafka_store'" in call_args
        assert "'topic' = 'derived_topic'" in call_args

    @pytest.mark.asyncio
    async def test_update_stream(
        self, mock_connection, mock_describe_result, sample_stream_data
    ):
        """Test updating stream."""
        manager = StreamManager(mock_connection)

        # Mock the query call for get() after update
        mock_connection.query.return_value = mock_describe_result(
            "stream", sample_stream_data
        )

        from deltastream_sdk.models import StreamUpdateParams

        params = StreamUpdateParams(comment="Updated comment")

        await manager.update("test_stream", params)

        expected_sql = "ALTER STREAM \"test_stream\" SET COMMENT 'Updated comment';"
        mock_connection.exec.assert_called_once_with(expected_sql)

    @pytest.mark.asyncio
    async def test_delete_stream(self, mock_connection):
        """Test deleting stream."""
        manager = StreamManager(mock_connection)

        await manager.delete("test_stream")

        expected_sql = 'DROP STREAM "test_stream";'
        mock_connection.exec.assert_called_once_with(expected_sql)

    @pytest.mark.asyncio
    async def test_exists_stream_true(self, mock_connection, mock_list_result):
        """Test stream exists check (true case)."""
        manager = StreamManager(mock_connection)
        mock_connection.query.return_value = mock_list_result(["test_stream"])

        exists = await manager.exists("test_stream")

        assert exists is True

    @pytest.mark.asyncio
    async def test_exists_stream_false(self, mock_connection, mock_list_result):
        """Test stream exists check (false case)."""
        manager = StreamManager(mock_connection)
        mock_connection.query.return_value = mock_list_result([])

        exists = await manager.exists("nonexistent_stream")

        assert exists is False

    @pytest.mark.asyncio
    async def test_start_stream(self, mock_connection):
        """Test starting stream."""
        manager = StreamManager(mock_connection)

        await manager.start("test_stream")

        expected_sql = 'START STREAM "test_stream";'
        mock_connection.exec.assert_called_once_with(expected_sql)

    @pytest.mark.asyncio
    async def test_stop_stream(self, mock_connection):
        """Test stopping stream."""
        manager = StreamManager(mock_connection)

        await manager.stop("test_stream")

        expected_sql = 'STOP STREAM "test_stream";'
        mock_connection.exec.assert_called_once_with(expected_sql)


class TestStoreManager:
    """Test StoreManager."""

    @pytest.mark.asyncio
    async def test_create_kafka_store(
        self, mock_connection, mock_describe_result, sample_store_data
    ):
        """Test creating Kafka store."""
        manager = StoreManager(mock_connection)

        # Mock the query call for get() after creation
        kafka_store_data = sample_store_data.copy()
        kafka_store_data["name"] = "kafka_store"
        mock_connection.query.return_value = mock_describe_result(
            "store", kafka_store_data
        )

        await manager.create_kafka_store(
            name="kafka_store",
            uris="localhost:9092",
            kafka_sasl_hash_function="PLAIN",
            kafka_sasl_username="user",
            kafka_sasl_password="pass",
        )

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE STORE "kafka_store"' in call_args
        assert "'type' = KAFKA" in call_args
        assert "'uris' = 'localhost:9092'" in call_args
        assert "'kafka.sasl.hash_function' = PLAIN" in call_args
        assert "'kafka.sasl.username' = 'user'" in call_args
        assert "'kafka.sasl.password' = 'pass'" in call_args

    @pytest.mark.asyncio
    async def test_create_kinesis_store(
        self, mock_connection, mock_describe_result, sample_store_data
    ):
        """Test creating Kinesis store."""
        manager = StoreManager(mock_connection)

        # Mock the query call for get() after creation
        kinesis_store_data = sample_store_data.copy()
        kinesis_store_data["name"] = "kinesis_store"
        mock_connection.query.return_value = mock_describe_result(
            "store", kinesis_store_data
        )

        await manager.create_kinesis_store(
            name="kinesis_store",
            uris="https://kinesis.us-east-1.amazonaws.com",
            kinesis_access_key_id="ACCESS_KEY",
            kinesis_secret_access_key="SECRET_KEY",
        )

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE STORE "kinesis_store"' in call_args
        assert "'type' = KINESIS" in call_args
        assert "'uris' = 'https://kinesis.us-east-1.amazonaws.com'" in call_args
        assert "'kinesis.access_key_id' = 'ACCESS_KEY'" in call_args
        assert "'kinesis.secret_access_key' = 'SECRET_KEY'" in call_args

    @pytest.mark.asyncio
    async def test_create_s3_store(
        self, mock_connection, mock_describe_result, sample_store_data
    ):
        """Test creating S3 store."""
        manager = StoreManager(mock_connection)

        # Mock the query call for get() after creation
        s3_store_data = sample_store_data.copy()
        s3_store_data["name"] = "s3_store"
        mock_connection.query.return_value = mock_describe_result(
            "store", s3_store_data
        )

        await manager.create_s3_store(
            name="s3_store",
            uris="https://mybucket.s3.us-west-2.amazonaws.com/",
            aws_access_key_id="ACCESS_KEY",
            aws_secret_access_key="SECRET_KEY",
        )

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE STORE "s3_store"' in call_args
        assert "'type' = S3" in call_args
        assert "'uris' = 'https://mybucket.s3.us-west-2.amazonaws.com/'" in call_args
        assert "'aws.access_key_id' = 'ACCESS_KEY'" in call_args
        assert "'aws.secret_access_key' = 'SECRET_KEY'" in call_args

    @pytest.mark.asyncio
    async def test_test_connection(self, mock_connection, mock_query_rows):
        """Test testing store connection."""
        manager = StoreManager(mock_connection)

        # Mock the query result for test connection
        mock_connection.query.return_value = mock_query_rows

        result = await manager.test_connection("test_store")

        expected_sql = 'TEST STORE "test_store";'
        mock_connection.query.assert_called_once_with(expected_sql)
        assert result == {"name": "test_stream"}

    @pytest.mark.asyncio
    async def test_get_topics(self, mock_connection, mock_list_result):
        """Test getting store topics."""
        manager = StoreManager(mock_connection)
        mock_connection.query.return_value = mock_list_result(["topic1", "topic2"])

        topics = await manager.get_topics("kafka_store")

        expected_sql = 'LIST TOPICS FROM STORE "kafka_store";'
        mock_connection.query.assert_called_once_with(expected_sql)
        assert topics == ["topic1", "topic2"]


class TestDatabaseManager:
    """Test DatabaseManager."""

    @pytest.mark.asyncio
    async def test_create_database(
        self, mock_connection, mock_describe_result, sample_database_data
    ):
        """Test creating database."""
        manager = DatabaseManager(mock_connection)

        # Mock the query call for get() after creation
        test_db_data = sample_database_data.copy()
        test_db_data["name"] = "test_db"
        mock_connection.query.return_value = mock_describe_result(
            "database", test_db_data
        )

        await manager.create(name="test_db", comment="Test database")

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE DATABASE "test_db"' in call_args
        assert "COMMENT 'Test database'" in call_args

    @pytest.mark.asyncio
    async def test_create_database_minimal(
        self, mock_connection, mock_describe_result, sample_database_data
    ):
        """Test creating database with minimal parameters."""
        manager = DatabaseManager(mock_connection)

        # Mock the query call for get() after creation
        minimal_db_data = sample_database_data.copy()
        minimal_db_data["name"] = "minimal_db"
        mock_connection.query.return_value = mock_describe_result(
            "database", minimal_db_data
        )

        await manager.create(name="minimal_db")

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE DATABASE "minimal_db"' in call_args
        # Should not contain WITH clause if no optional params
        assert "WITH" not in call_args

    @pytest.mark.asyncio
    async def test_get_database(
        self, mock_connection, mock_describe_result, sample_database_data
    ):
        """Test getting database."""
        manager = DatabaseManager(mock_connection)
        mock_connection.query.return_value = mock_describe_result(
            "DATABASE", sample_database_data
        )

        database = await manager.get("test_database")

        expected_sql = 'DESCRIBE DATABASE "test_database";'
        mock_connection.query.assert_called_once_with(expected_sql)
        assert isinstance(database, Database)
        assert database.name == "test_database"

    @pytest.mark.asyncio
    async def test_update_database(
        self, mock_connection, mock_describe_result, sample_database_data
    ):
        """Test updating database."""
        manager = DatabaseManager(mock_connection)

        # Mock the query call for get() after update
        test_db_data = sample_database_data.copy()
        test_db_data["name"] = "test_db"
        mock_connection.query.return_value = mock_describe_result(
            "database", test_db_data
        )

        await manager.update("test_db", comment="Updated comment")

        expected_sql = "ALTER DATABASE \"test_db\" SET COMMENT 'Updated comment';"
        mock_connection.exec.assert_called_once_with(expected_sql)

    @pytest.mark.asyncio
    async def test_delete_database(self, mock_connection):
        """Test deleting database."""
        manager = DatabaseManager(mock_connection)

        await manager.delete("test_db")

        expected_sql = 'DROP DATABASE "test_db";'
        mock_connection.exec.assert_called_once_with(expected_sql)


class TestComputePoolManager:
    """Test ComputePoolManager."""

    @pytest.mark.asyncio
    async def test_create_compute_pool(
        self, mock_connection, mock_describe_result, sample_compute_pool_data
    ):
        """Test creating compute pool."""
        manager = ComputePoolManager(mock_connection)

        # Mock the query call for get() after creation
        test_pool_data = sample_compute_pool_data.copy()
        test_pool_data["name"] = "test_pool"
        mock_connection.query.return_value = mock_describe_result(
            "compute_pool", test_pool_data
        )

        await manager.create(
            name="test_pool",
            size="MEDIUM",
            min_units=1,
            max_units=5,
            auto_suspend=True,
            auto_suspend_minutes=15,
        )

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE COMPUTE_POOL "test_pool"' in call_args
        assert "'size' = 'MEDIUM'" in call_args
        assert "'min.units' = '1'" in call_args
        assert "'max.units' = '5'" in call_args
        assert "'auto.suspend' = 'true'" in call_args
        assert "'auto.suspend.minutes' = '15'" in call_args

    @pytest.mark.asyncio
    async def test_start_compute_pool(self, mock_connection):
        """Test starting compute pool."""
        manager = ComputePoolManager(mock_connection)

        await manager.start("test_pool")

        expected_sql = 'START COMPUTE_POOL "test_pool";'
        mock_connection.exec.assert_called_once_with(expected_sql)

    @pytest.mark.asyncio
    async def test_stop_compute_pool(self, mock_connection):
        """Test stopping compute pool."""
        manager = ComputePoolManager(mock_connection)

        await manager.stop("test_pool")

        expected_sql = 'STOP COMPUTE_POOL "test_pool";'
        mock_connection.exec.assert_called_once_with(expected_sql)

    @pytest.mark.asyncio
    async def test_get_compute_pool(
        self, mock_connection, mock_describe_result, sample_compute_pool_data
    ):
        """Test getting compute pool."""
        manager = ComputePoolManager(mock_connection)
        mock_connection.query.return_value = mock_describe_result(
            "COMPUTE_POOL", sample_compute_pool_data
        )

        pool = await manager.get("test_pool")

        expected_sql = 'DESCRIBE COMPUTE_POOL "test_pool";'
        mock_connection.query.assert_called_once_with(expected_sql)
        assert isinstance(pool, ComputePool)
        assert pool.name == "test_pool"

    @pytest.mark.asyncio
    async def test_update_compute_pool(
        self, mock_connection, mock_describe_result, sample_compute_pool_data
    ):
        """Test updating compute pool."""
        manager = ComputePoolManager(mock_connection)

        # Mock the query call for get() after update
        test_pool_data = sample_compute_pool_data.copy()
        test_pool_data["name"] = "test_pool"
        mock_connection.query.return_value = mock_describe_result(
            "compute_pool", test_pool_data
        )

        from deltastream_sdk.models import ComputePoolUpdateParams

        params = ComputePoolUpdateParams(
            min_units=2, max_units=10, auto_suspend_minutes=30
        )

        await manager.update("test_pool", params)

        call_args = mock_connection.exec.call_args[0][0]
        assert 'UPDATE COMPUTE_POOL "test_pool"' in call_args
        assert "'min.units' = '2'" in call_args
        assert "'max.units' = '10'" in call_args
        assert "'auto.suspend.minutes' = '30'" in call_args


class TestEntityManager:
    """Test EntityManager operations."""

    @pytest.mark.asyncio
    async def test_insert_values(self, mock_connection):
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        await manager.insert_values(
            name="my_entity",
            values=[
                {"pageId": 10, "pageviews": 123},
                {"pageId": 15, "pageviews": 256},
            ],
            store="my_store",
        )

        # With multiple values, the method makes separate calls for each value
        assert mock_connection.exec.call_count == 2

        # Check the calls were made with the expected SQL
        calls = mock_connection.exec.call_args_list
        first_call = calls[0][0][0]
        second_call = calls[1][0][0]

        assert 'INSERT INTO ENTITY "my_entity"' in first_call
        assert 'IN STORE "my_store"' in first_call
        assert '(\'{"pageId": 10, "pageviews": 123}\')' in first_call

        assert 'INSERT INTO ENTITY "my_entity"' in second_call
        assert 'IN STORE "my_store"' in second_call
        assert '(\'{"pageId": 15, "pageviews": 256}\')' in second_call

    @pytest.mark.asyncio
    async def test_insert_values_with_extra_with_params(self, mock_connection):
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        await manager.insert_values(
            name="my_entity",
            values=['{"k": "v"}'],
            store="my_store",
            with_params={"topic": "my_topic"},
        )

        call_args = mock_connection.exec.call_args[0][0]
        assert 'INSERT INTO ENTITY "my_entity"' in call_args
        assert 'IN STORE "my_store"' in call_args
        assert "'topic' = 'my_topic'" in call_args
        assert '(\'{"k": "v"}\')' in call_args
        # Ensure proper SQL structure: INSERT INTO ENTITY ... IN STORE ... VALUE(...) WITH (...)
        expected_pattern = 'INSERT INTO ENTITY "my_entity" IN STORE "my_store" VALUE'
        assert expected_pattern in call_args
        assert "WITH (" in call_args

    @pytest.mark.asyncio
    async def test_insert_values_single_value_exact_sql(self, mock_connection):
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        await manager.insert_values(
            name="test-sdk",
            values=[
                {"viewtime": 1753311018649, "userid": "User_3", "pageid": "Page_1"}
            ],
            store="ChristopheKafka",
        )

        call_args = mock_connection.exec.call_args[0][0]
        # Verify the exact SQL format matches the expected syntax
        expected_start = (
            'INSERT INTO ENTITY "test-sdk" IN STORE "ChristopheKafka" VALUE('
        )
        assert call_args.startswith(expected_start)
        assert '"viewtime": 1753311018649' in call_args
        assert '"userid": "User_3"' in call_args
        assert '"pageid": "Page_1"' in call_args

    @pytest.mark.asyncio
    async def test_create_entity_with_defaults(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating a Kafka entity with default parameters."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "pv"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(name="pv")

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE ENTITY "pv"' in call_args
        # No IN STORE or WITH clauses for defaults
        assert "IN STORE" not in call_args
        assert "WITH" not in call_args

    @pytest.mark.asyncio
    async def test_create_entity_with_store(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating entity in a specific store."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "pv"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(name="pv", store="demostore")

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE ENTITY "pv"' in call_args
        assert 'IN STORE "demostore"' in call_args

    @pytest.mark.asyncio
    async def test_create_kafka_entity_with_passthrough_config(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating Kafka entity with retention and other topic configurations."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "customers"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(
            name="customers",
            store="kafka_store",
            params={
                "topic.partitions": "1",
                "topic.replicas": "2",
                "kafka.topic.retention.ms": "172800000",
            },
        )

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE ENTITY "customers"' in call_args
        assert 'IN STORE "kafka_store"' in call_args
        assert "WITH (" in call_args
        assert "'topic.partitions' = '1'" in call_args
        assert "'topic.replicas' = '2'" in call_args
        assert "'kafka.topic.retention.ms' = '172800000'" in call_args

    @pytest.mark.asyncio
    async def test_create_kafka_entity_with_cleanup_policy(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating Kafka entity with partitions, replicas, and cleanup policy."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "pv_compact"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(
            name="pv_compact",
            params={
                "topic.partitions": "2",
                "topic.replicas": "1",
                "kafka.topic.cleanup.policy": "compact",
            },
        )

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE ENTITY "pv_compact"' in call_args
        assert "WITH (" in call_args
        assert "'topic.partitions' = '2'" in call_args
        assert "'topic.replicas' = '1'" in call_args
        assert "'kafka.topic.cleanup.policy' = 'compact'" in call_args

    @pytest.mark.asyncio
    async def test_create_entity_with_protobuf_descriptors(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating entity with key and value ProtoBuf descriptors."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "pv_pb"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(
            name="pv_pb",
            params={
                "key.descriptor": 'pb_key."PageviewsKey"',
                "value.descriptor": 'pb_value."Pageviews"',
            },
        )

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE ENTITY "pv_pb"' in call_args
        assert "WITH (" in call_args
        assert "'key.descriptor' = 'pb_key.\"PageviewsKey\"'" in call_args
        assert "'value.descriptor' = 'pb_value.\"Pageviews\"'" in call_args

    @pytest.mark.asyncio
    async def test_create_kinesis_entity_with_shards(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating Kinesis entity with shards parameter."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "pv_kinesis"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(
            name="pv_kinesis", store="kinesis_store", params={"kinesis.shards": "3"}
        )

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE ENTITY "pv_kinesis"' in call_args
        assert 'IN STORE "kinesis_store"' in call_args
        assert "WITH (" in call_args
        assert "'kinesis.shards' = '3'" in call_args

    @pytest.mark.asyncio
    async def test_create_snowflake_database(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating a Snowflake database (case-sensitive name)."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "DELTA_STREAMING"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(name="DELTA_STREAMING")

        call_args = mock_connection.exec.call_args[0][0]
        # Name should be properly quoted to preserve case
        assert 'CREATE ENTITY "DELTA_STREAMING"' in call_args

    @pytest.mark.asyncio
    async def test_create_snowflake_schema_in_database(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating a Snowflake schema within a database."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "DELTA_STREAMING.MY_STREAMING_SCHEMA"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(name="DELTA_STREAMING.MY_STREAMING_SCHEMA")

        call_args = mock_connection.exec.call_args[0][0]
        # Hierarchical name should be properly quoted
        assert 'CREATE ENTITY "DELTA_STREAMING.MY_STREAMING_SCHEMA"' in call_args

    @pytest.mark.asyncio
    async def test_create_databricks_catalog(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating a Databricks catalog."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "cat1"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(name="cat1")

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE ENTITY "cat1"' in call_args

    @pytest.mark.asyncio
    async def test_create_databricks_schema_in_catalog(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating a Databricks schema within a catalog."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "cat1.schema1"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(name="cat1.schema1")

        call_args = mock_connection.exec.call_args[0][0]
        # Hierarchical name should be properly quoted
        assert 'CREATE ENTITY "cat1.schema1"' in call_args

    @pytest.mark.asyncio
    async def test_create_entity_with_all_parameters(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating entity with all possible parameters."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "complex_entity"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(
            name="complex_entity",
            store="kafka_store",
            params={
                "topic.partitions": "3",
                "topic.replicas": "2",
                "kafka.topic.retention.ms": "604800000",
                "kafka.topic.cleanup.policy": "delete",
                "key.descriptor": "pb_key.MyKey",
                "value.descriptor": "pb_value.MyValue",
            },
        )

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE ENTITY "complex_entity"' in call_args
        assert 'IN STORE "kafka_store"' in call_args
        assert "WITH (" in call_args
        assert "'topic.partitions' = '3'" in call_args
        assert "'topic.replicas' = '2'" in call_args
        assert "'kafka.topic.retention.ms' = '604800000'" in call_args
        assert "'kafka.topic.cleanup.policy' = 'delete'" in call_args
        assert "'key.descriptor' = 'pb_key.MyKey'" in call_args
        assert "'value.descriptor' = 'pb_value.MyValue'" in call_args

    @pytest.mark.asyncio
    async def test_create_entity_with_case_sensitive_store_name(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test creating entity with case-sensitive store name."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = "my_entity"
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(name="my_entity", store="MySpecialStore")

        call_args = mock_connection.exec.call_args[0][0]
        assert 'CREATE ENTITY "my_entity"' in call_args
        # Store name should be properly quoted to preserve case
        assert 'IN STORE "MySpecialStore"' in call_args

    @pytest.mark.asyncio
    async def test_create_entity_escapes_special_characters(
        self, mock_connection, mock_describe_result, sample_entity_data
    ):
        """Test that entity names with special characters are properly escaped."""
        from deltastream_sdk.resources import EntityManager

        manager = EntityManager(mock_connection)

        # Mock the query call for get() after creation
        entity_data = sample_entity_data.copy()
        entity_data["name"] = 'entity"with"quotes'
        mock_connection.query.return_value = mock_describe_result("entity", entity_data)

        await manager.create(name='entity"with"quotes')

        call_args = mock_connection.exec.call_args[0][0]
        # Quotes in the name should be escaped (doubled)
        assert 'CREATE ENTITY "entity""with""quotes"' in call_args
