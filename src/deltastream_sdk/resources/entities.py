"""Entity resource manager for DeltaStream SDK."""

from typing import List, Union, Dict, Any, Optional
import json

from .base import BaseResourceManager
from ..models.entities import (
    Entity,
    EntityCreateParams,
    EntityUpdateParams,
)


class EntityManager(BaseResourceManager[Entity]):
    """Manager for DeltaStream entity resources."""

    def __init__(self, connection):
        super().__init__(connection, Entity)

    def _get_list_sql(self, **filters) -> str:
        return "LIST ENTITIES"

    def _get_describe_sql(self, name: str) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DESCRIBE ENTITY {escaped_name}"

    def _get_create_sql(self, **params) -> str:
        if isinstance(params.get("params"), EntityCreateParams):
            create_params = params["params"]
        else:
            create_params = EntityCreateParams(**params)

        name = self._escape_identifier(create_params.name)
        sql = f"CREATE ENTITY {name}"

        if create_params.store:
            escaped_store = self._escape_identifier(create_params.store)
            sql += f" IN STORE {escaped_store}"

        with_parts = []

        if create_params.params:
            for key, value in create_params.params.items():
                with_parts.append(f"'{key}' = {self._escape_string(str(value))}")

        if with_parts:
            sql += f" WITH ({', '.join(with_parts)})"

        return sql

    def _get_update_sql(self, name: str, **params) -> str:
        escaped_name = self._escape_identifier(name)
        if isinstance(params.get("params"), EntityUpdateParams):
            update_params = params["params"]
        else:
            update_params = EntityUpdateParams(**params)

        if update_params.schema_definition:
            return f"UPDATE ENTITY {escaped_name} SET SCHEMA ({update_params.schema_definition})"
        return f"-- No updates for entity {name}"

    def _get_delete_sql(self, name: str, **params) -> str:
        escaped_name = self._escape_identifier(name)
        return f"DROP ENTITY {escaped_name}"

    async def insert_values(
        self,
        name: str,
        values: List[Union[str, Dict[str, Any]]],
        *,
        store: Optional[str] = None,
        with_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Insert JSON records into an entity via VALUE(S).

        Supports a single-record `VALUE('...')` syntax when `values` contains
        exactly one item. The `store` parameter will add an `IN STORE` clause,
        and `with_params` will be added as a `WITH` clause when provided.
        """
        if not values:
            raise ValueError("values must contain at least one record")

        escaped_name = self._escape_identifier(name)

        def to_json_string(record: Union[str, Dict[str, Any]]) -> str:
            if isinstance(record, str):
                return record
            return json.dumps(record, separators=(", ", ": "))

        if len(values) == 1:
            single = values[0]
            json_str = self._escape_string(to_json_string(single))
            sql = f"INSERT INTO ENTITY {escaped_name}"

            # Add IN STORE clause if store is provided
            if store:
                escaped_store = self._escape_identifier(store)
                sql += f" IN STORE {escaped_store}"

            sql += f" VALUE({json_str})"

            # Add WITH clause if we have with_params
            if with_params:
                with_parts = []
                for key, value in with_params.items():
                    with_parts.append(f"'{key}' = {self._escape_string(str(value))}")
                sql += f" WITH ({', '.join(with_parts)})"
        else:
            for record in values:
                json_str = self._escape_string(to_json_string(record))
                single_sql = f"INSERT INTO ENTITY {escaped_name}"

                # Add IN STORE clause if store is provided
                if store:
                    escaped_store = self._escape_identifier(store)
                    single_sql += f" IN STORE {escaped_store}"

                single_sql += f" VALUE({json_str})"

                # Add WITH clause if we have with_params
                if with_params:
                    with_parts = []
                    for key, value in with_params.items():
                        with_parts.append(
                            f"'{key}' = {self._escape_string(str(value))}"
                        )
                    single_sql += f" WITH ({', '.join(with_parts)})"

                await self._execute_sql(single_sql)
            return

        await self._execute_sql(sql)

    async def list_entities(
        self,
        store: Optional[str] = None,
        entity_path: Optional[str] = None,
    ) -> List[Entity]:
        """
        List entities, optionally in a specific store or under a specific entity path.

        Args:
            store: Optional store name to list entities from (uses current store if not provided)
            entity_path: Optional entity path to list entities under (lists root entities if not provided)

        Returns:
            List of Entity objects representing the entities
        """
        sql = "LIST ENTITIES"

        # Add IN clause for entity path if provided
        if entity_path:
            escaped_path = self._escape_identifier(entity_path)
            sql += f" IN {escaped_path}"

        # Add IN STORE clause if provided
        if store:
            escaped_store = self._escape_identifier(store)
            sql += f" IN STORE {escaped_store}"

        try:
            results = await self._query_sql(sql)
            entities = []
            for result in results:
                # Create Entity with the information from LIST ENTITIES
                entity = Entity(
                    name=result.get("Name", result.get("name", "")),
                    # Add is_leaf information if available
                )
                # Add any additional properties from the result
                if "Is Leaf" in result:
                    entity.is_leaf = result["Is Leaf"]
                elif "is_leaf" in result:
                    entity.is_leaf = result["is_leaf"]

                entities.append(entity)
            return entities
        except Exception as e:
            from ..exceptions import SQLError

            raise SQLError(f"Failed to list entities: {e}") from e
