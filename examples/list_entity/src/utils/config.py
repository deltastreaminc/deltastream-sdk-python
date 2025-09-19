"""Configuration management utilities for DeltaStream SDK examples."""

import base64
import json
import os
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()


def _decode_jwt_payload(token: str) -> Optional[dict]:
    """Decode JWT token payload (without verification)."""
    try:
        # Split the token
        parts = token.split(".")
        if len(parts) != 3:
            return None

        # Decode the payload (add padding if needed)
        payload = parts[1]
        # Add padding if needed
        payload += "=" * (4 - len(payload) % 4)

        decoded_bytes = base64.urlsafe_b64decode(payload)
        payload_data = json.loads(decoded_bytes.decode("utf-8"))

        return payload_data
    except Exception:
        return None


def _check_token_expiry(token: str) -> tuple[bool, Optional[datetime]]:
    """Check if JWT token is expired. Returns (is_valid, expiry_date)."""
    payload = _decode_jwt_payload(token)
    if not payload or "exp" not in payload:
        return False, None

    expiry_timestamp = payload["exp"]
    expiry_date = datetime.fromtimestamp(expiry_timestamp)
    now = datetime.now()

    is_valid = expiry_date > now
    return is_valid, expiry_date


class Config:
    """Configuration class for DeltaStream SDK examples."""

    def __init__(self) -> None:
        """Initialize configuration from environment variables."""
        self.auth_token: Optional[str] = os.getenv("DELTASTREAM_TOKEN")
        self.organization_id: Optional[str] = os.getenv("DELTASTREAM_ORG_ID")
        self.server_url: Optional[str] = os.getenv("DELTASTREAM_SERVER_URL")
        self.database_name: Optional[str] = os.getenv("DELTASTREAM_DATABASE_NAME")
        self.schema_name: Optional[str] = os.getenv("DELTASTREAM_SCHEMA_NAME")

    def validate(self) -> None:
        """Validate that required configuration is present."""
        if not self.auth_token:
            raise ValueError("DELTASTREAM_TOKEN environment variable is required")
        if not self.organization_id:
            raise ValueError("DELTASTREAM_ORG_ID environment variable is required")

        # Check token validity
        self._validate_token()

    def _validate_token(self) -> None:
        """Validate JWT token and check expiry."""
        if not self.auth_token:
            return

        is_valid, expiry_date = _check_token_expiry(self.auth_token)

        if not is_valid:
            if expiry_date:
                error_msg = (
                    f"❌ JWT token is expired! Token expired on {expiry_date}. "
                    "Please get a new token from your DeltaStream dashboard and update "
                    "the DELTASTREAM_TOKEN environment variable."
                )
            else:
                error_msg = (
                    "❌ JWT token is invalid or malformed. "
                    "Please check the DELTASTREAM_TOKEN environment variable."
                )
            raise ValueError(error_msg)

        print(f"✅ Token is valid until {expiry_date}")

    def get_connection_info(self) -> str:
        """Get a string description of the connection configuration."""
        server_info = self.server_url or "https://api.deltastream.io/v2"
        return f"Server: {server_info}, Org: {self.organization_id}"


def get_config() -> Config:
    """Get and validate configuration."""
    config = Config()
    print("Config setup: ", config.get_connection_info())
    config.validate()
    return config
