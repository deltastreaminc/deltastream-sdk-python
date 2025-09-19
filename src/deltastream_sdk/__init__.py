"""
DeltaStream Python SDK

A Python SDK for managing DeltaStream resources and operations.
Provides a high-level interface similar to Databricks SDK for DeltaStream's
SQL-based streaming platform.
"""

from .client import DeltaStreamClient
from .exceptions import (
    DeltaStreamSDKError,
    ResourceNotFound,
    ResourceAlreadyExists,
    InvalidConfiguration,
    ConnectionError,
)

# Re-export modules for backward compatibility and convenience
from . import models, resources, exceptions

__all__ = [
    "DeltaStreamClient",
    "DeltaStreamSDKError",
    "ResourceNotFound",
    "ResourceAlreadyExists",
    "InvalidConfiguration",
    "ConnectionError",
    "models",
    "resources",
    "exceptions",
]
