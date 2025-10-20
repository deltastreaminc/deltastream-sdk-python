"""
Base resource manager for DeltaStream SDK.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, TypeVar, Generic, Type

from ..models.base import BaseModel
from ..exceptions import ResourceNotFound, SQLError, ConnectionError
from deltastream.api.conn import APIConnection


T = TypeVar("T", bound=BaseModel)


class BaseResourceManager(ABC, Generic[T]):
    """Base class for all DeltaStream resource managers."""

    def __init__(self, connection: APIConnection, model_class: Type[T]):
        self._connection = connection
        self._model_class = model_class
        self.connection = connection  # Expose connection for tests

    @abstractmethod
    def _get_list_sql(self, **filters) -> str:
        """Generate SQL for listing resources."""
        pass

    @abstractmethod
    def _get_describe_sql(self, name: str) -> str:
        """Generate SQL for describing a specific resource."""
        pass

    @abstractmethod
    def _get_create_sql(self, **params) -> str:
        """Generate SQL for creating a resource."""
        pass

    @abstractmethod
    def _get_update_sql(self, name: str, **params) -> str:
        """Generate SQL for updating a resource."""
        pass

    @abstractmethod
    def _get_delete_sql(self, name: str, **params) -> str:
        """Generate SQL for deleting a resource."""
        pass

    async def list(self, **filters) -> List[T]:
        """List all resources of this type."""
        try:
            sql = self._get_list_sql(**filters)
            results = await self._query_sql(sql)
            return [self._model_class.from_dict(result) for result in results]  # type: ignore[misc]
        except Exception as e:
            raise SQLError(f"Failed to list resources: {e}") from e

    async def get(self, name: str) -> T:
        """Get a specific resource by name."""
        try:
            sql = self._get_describe_sql(name)
            results = await self._query_sql(sql)
            if not results:
                raise ResourceNotFound(f"Resource '{name}' not found")

            # Convert DESCRIBE results to a single dictionary
            resource_dict = self._convert_describe_to_dict(results)
            # Ensure the resource has a name
            if "Name" not in resource_dict:
                resource_dict["Name"] = name

            return self._model_class.from_dict(resource_dict)  # type: ignore[return-value]
        except ResourceNotFound:
            raise
        except Exception as e:
            raise SQLError(f"Failed to get resource '{name}': {e}") from e

    async def create(self, **params) -> T:
        """Create a new resource."""
        try:
            sql = self._get_create_sql(**params)
            await self._execute_sql(sql)

            # Return the created resource
            name = params.get("name")
            # If name not in params directly, check if there's a params object with name
            if not name and "params" in params:
                param_obj = params["params"]
                if hasattr(param_obj, "name"):
                    name = param_obj.name

            if name:
                return await self.get(name)
            else:
                raise SQLError("Resource name not provided for creation")

        except Exception as e:
            raise SQLError(f"Failed to create resource: {e}") from e

    async def update(self, name: str, params=None, **kwargs) -> T:
        """Update an existing resource."""
        try:
            if params is not None:
                # If params object is provided, convert to dict
                if hasattr(params, "to_dict"):
                    update_params = params.to_dict()
                elif hasattr(params, "__dict__"):
                    update_params = params.__dict__
                else:
                    update_params = params
                sql = self._get_update_sql(name, **update_params)
            else:
                sql = self._get_update_sql(name, **kwargs)
            await self._execute_sql(sql)
            return await self.get(name)
        except Exception as e:
            raise SQLError(f"Failed to update resource '{name}': {e}") from e

    async def delete(self, name: str, **params) -> None:
        """Delete a resource."""
        try:
            sql = self._get_delete_sql(name, **params)
            await self._execute_sql(sql)
        except Exception as e:
            raise SQLError(f"Failed to delete resource '{name}': {e}") from e

    async def exists(self, name: str) -> bool:
        """Check if a resource exists."""
        try:
            # Use describe syntax to check if resource exists
            await self.get(name)
            return True
        except ResourceNotFound:
            return False
        except Exception:
            # If there's an error checking existence, assume it doesn't exist
            return False

    # Helper methods for SQL execution
    async def _execute_sql(self, sql: str) -> None:
        """Execute SQL statement without return."""
        try:
            sql = self._ensure_semicolon(sql)
            await self._connection.exec(sql)
        except Exception as e:
            raise ConnectionError(f"Failed to execute SQL: {e}") from e

    async def _query_sql(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results."""
        try:
            sql = self._ensure_semicolon(sql)
            rows = await self._connection.query(sql)
            results = []

            # Get column names
            columns = [col.name for col in rows.columns()]

            # Convert rows to dictionaries
            async for row in rows:
                if row:
                    result_dict = dict(zip(columns, row))
                    results.append(result_dict)

            return results
        except Exception as e:
            raise ConnectionError(f"Failed to query SQL: {e}") from e

    def _convert_describe_to_dict(
        self, describe_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Convert DESCRIBE query results (key-value pairs) to a single dictionary."""
        result = {}
        for row in describe_results:
            # DESCRIBE queries return rows with 'property' and 'value' columns
            if "property" in row and "value" in row:
                result[row["property"]] = row["value"]
            # Also handle other column name variations
            elif len(row) == 2:
                keys = list(row.keys())
                result[row[keys[0]]] = row[keys[1]]
        return result

    def _escape_identifier(self, identifier: str) -> str:
        """Escape SQL identifier (table/column names)."""
        # Escape double quotes by doubling them
        escaped_identifier = identifier.replace('"', '""')
        return f'"{escaped_identifier}"'

    def _escape_string(self, value: str) -> str:
        """Escape SQL string literal."""
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"

    def _ensure_semicolon(self, sql: str) -> str:
        """Ensure SQL statement ends with a semicolon."""
        sql = sql.strip()
        if not sql.endswith(";"):
            sql += ";"
        return sql
