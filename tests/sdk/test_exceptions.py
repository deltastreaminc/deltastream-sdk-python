"""
Tests for SDK exceptions.
"""

from deltastream_sdk.exceptions import (
    DeltaStreamSDKError,
    ResourceNotFound,
    ResourceAlreadyExists,
    InvalidConfiguration,
    ConnectionError,
    SQLError,
    PermissionError,
    ResourceInUse,
)


class TestDeltaStreamSDKError:
    """Test base SDK exception."""

    def test_basic_exception(self):
        """Test basic exception creation."""
        exc = DeltaStreamSDKError("Test error message")

        assert str(exc) == "Test error message"
        assert isinstance(exc, Exception)

    def test_exception_with_cause(self):
        """Test exception with underlying cause."""
        original_error = ValueError("Original error")

        try:
            raise DeltaStreamSDKError("SDK error") from original_error
        except DeltaStreamSDKError as exc:
            assert str(exc) == "SDK error"
            assert exc.__cause__ == original_error

    def test_exception_inheritance(self):
        """Test that all SDK exceptions inherit from base."""
        exceptions = [
            ResourceNotFound("test"),
            ResourceAlreadyExists("test"),
            InvalidConfiguration("test"),
            ConnectionError("test"),
            SQLError("test"),
            PermissionError("test"),
            ResourceInUse("test"),
        ]

        for exc in exceptions:
            assert isinstance(exc, DeltaStreamSDKError)
            assert isinstance(exc, Exception)


class TestResourceNotFound:
    """Test ResourceNotFound exception."""

    def test_basic_not_found(self):
        """Test basic not found exception."""
        exc = ResourceNotFound("Stream 'test_stream' not found")

        assert str(exc) == "Stream 'test_stream' not found"
        assert isinstance(exc, DeltaStreamSDKError)

    def test_not_found_with_resource_name(self):
        """Test not found with resource name formatting."""
        exc = ResourceNotFound("Stream 'test_stream' not found")

        # The exception should format the message appropriately
        assert "test_stream" in str(exc)
        assert "stream" in str(exc).lower()


class TestResourceAlreadyExists:
    """Test ResourceAlreadyExists exception."""

    def test_basic_already_exists(self):
        """Test basic already exists exception."""
        exc = ResourceAlreadyExists("Stream 'test_stream' already exists")

        assert str(exc) == "Stream 'test_stream' already exists"
        assert isinstance(exc, DeltaStreamSDKError)

    def test_already_exists_with_resource_name(self):
        """Test already exists with resource name formatting."""
        exc = ResourceAlreadyExists("Stream 'test_stream' already exists")

        assert "test_stream" in str(exc)
        assert "already exists" in str(exc).lower()


class TestInvalidConfiguration:
    """Test InvalidConfiguration exception."""

    def test_basic_invalid_config(self):
        """Test basic invalid configuration exception."""
        exc = InvalidConfiguration("Invalid bootstrap servers configuration")

        assert str(exc) == "Invalid bootstrap servers configuration"
        assert isinstance(exc, DeltaStreamSDKError)

    def test_invalid_config_with_details(self):
        """Test invalid configuration with detailed message."""
        exc = InvalidConfiguration(
            "Invalid store configuration: bootstrap.servers is required for Kafka stores"
        )

        assert "bootstrap.servers" in str(exc)
        assert "required" in str(exc)


class TestConnectionError:
    """Test ConnectionError exception."""

    def test_basic_connection_error(self):
        """Test basic connection error."""
        exc = ConnectionError("Failed to connect to DeltaStream server")

        assert str(exc) == "Failed to connect to DeltaStream server"
        assert isinstance(exc, DeltaStreamSDKError)

    def test_connection_error_with_details(self):
        """Test connection error with detailed information."""
        exc = ConnectionError("Connection timeout after 30 seconds")

        assert "timeout" in str(exc)
        assert "30 seconds" in str(exc)


class TestSQLError:
    """Test SQLError exception."""

    def test_basic_sql_error(self):
        """Test basic SQL error."""
        exc = SQLError("Invalid SQL syntax in CREATE STREAM statement")

        assert str(exc) == "Invalid SQL syntax in CREATE STREAM statement"
        assert isinstance(exc, DeltaStreamSDKError)

    def test_sql_error_with_query(self):
        """Test SQL error with query information."""
        query = "CREATE STREAM test WITH ('invalid' = 'params')"
        exc = SQLError(f"SQL execution failed: {query}")

        assert query in str(exc)
        assert "SQL execution failed" in str(exc)


class TestPermissionError:
    """Test PermissionError exception."""

    def test_basic_permission_error(self):
        """Test basic permission error."""
        exc = PermissionError("Insufficient permissions to create stream")

        assert str(exc) == "Insufficient permissions to create stream"
        assert isinstance(exc, DeltaStreamSDKError)

    def test_permission_error_with_resource(self):
        """Test permission error with specific resource."""
        exc = PermissionError(
            "Permission denied: cannot delete stream 'production_data'"
        )

        assert "Permission denied" in str(exc)
        assert "production_data" in str(exc)


class TestResourceInUse:
    """Test ResourceInUse exception."""

    def test_basic_resource_in_use(self):
        """Test basic resource in use error."""
        exc = ResourceInUse("Cannot delete store: it is being used by active streams")

        assert str(exc) == "Cannot delete store: it is being used by active streams"
        assert isinstance(exc, DeltaStreamSDKError)

    def test_resource_in_use_with_details(self):
        """Test resource in use with detailed information."""
        exc = ResourceInUse(
            "Store 'kafka_prod' is in use by streams: ['orders', 'payments', 'users']"
        )

        assert "kafka_prod" in str(exc)
        assert "orders" in str(exc)
        assert "in use" in str(exc)


class TestExceptionChaining:
    """Test exception chaining and conversion."""

    def test_exception_from_api_error(self):
        """Test creating SDK exception from API error."""
        api_error = Exception("API returned 500: Internal Server Error")

        try:
            raise ConnectionError("Failed to connect to DeltaStream") from api_error
        except ConnectionError as sdk_error:
            assert isinstance(sdk_error, DeltaStreamSDKError)
            assert sdk_error.__cause__ == api_error
            assert "Failed to connect" in str(sdk_error)

    def test_exception_chain_preservation(self):
        """Test that exception chains are preserved."""
        original = ValueError("Invalid value")
        intermediate = None

        try:
            try:
                raise SQLError("SQL parsing failed") from original
            except SQLError as caught_intermediate:
                intermediate = caught_intermediate
                raise DeltaStreamSDKError("Operation failed") from intermediate
        except DeltaStreamSDKError as final:
            assert final.__cause__ == intermediate
            assert intermediate.__cause__ == original
            assert isinstance(final, DeltaStreamSDKError)

    def test_nested_exception_access(self):
        """Test accessing nested exception information."""
        root_cause = ConnectionError("Network timeout")

        try:
            raise DeltaStreamSDKError("Stream creation failed") from root_cause
        except DeltaStreamSDKError as wrapper:
            # Should be able to access the root cause
            assert wrapper.__cause__ == root_cause
            assert isinstance(wrapper.__cause__, ConnectionError)
            assert "Network timeout" in str(wrapper.__cause__)


class TestExceptionMessages:
    """Test exception message formatting."""

    def test_resource_not_found_formatting(self):
        """Test ResourceNotFound message formatting."""
        # Test with just resource name
        exc1 = ResourceNotFound("test_stream")
        assert "test_stream" in str(exc1)

        # Test with resource type
        exc2 = ResourceNotFound("test_stream not found")
        assert "test_stream not found" == str(exc2)

    def test_consistent_error_messages(self):
        """Test that error messages are consistent and informative."""
        exceptions = [
            ResourceNotFound("Resource not found"),
            ResourceAlreadyExists("Resource already exists"),
            InvalidConfiguration("Invalid configuration"),
            ConnectionError("Connection failed"),
            SQLError("SQL error occurred"),
            PermissionError("Permission denied"),
            ResourceInUse("Resource is in use"),
        ]

        for exc in exceptions:
            # All messages should be non-empty strings
            assert isinstance(str(exc), str)
            assert len(str(exc)) > 0

            # Messages should not contain placeholder text
            assert "TODO" not in str(exc)
            assert "FIXME" not in str(exc)
