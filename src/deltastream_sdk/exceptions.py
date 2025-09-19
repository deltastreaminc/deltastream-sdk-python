"""
Exception classes for the DeltaStream SDK.
"""


class DeltaStreamSDKError(Exception):
    """Base exception class for all DeltaStream SDK errors."""

    pass


class ResourceNotFound(DeltaStreamSDKError):
    """Raised when a requested resource does not exist."""

    pass


class ResourceAlreadyExists(DeltaStreamSDKError):
    """Raised when trying to create a resource that already exists."""

    pass


class InvalidConfiguration(DeltaStreamSDKError):
    """Raised when resource configuration is invalid."""

    pass


class ConnectionError(DeltaStreamSDKError):
    """Raised when there are connection issues with DeltaStream."""

    pass


class SQLError(DeltaStreamSDKError):
    """Raised when there are SQL execution errors."""

    pass


class PermissionError(DeltaStreamSDKError):
    """Raised when the user lacks permissions for an operation."""

    pass


class ResourceInUse(DeltaStreamSDKError):
    """Raised when trying to delete a resource that is in use."""

    pass
