from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from sqlalchemy.dialects import sqlite

from local_data_api.models import ColumnMetadata, Field
from local_data_api.resources.resource import Resource, register_resource_type

if TYPE_CHECKING:  # pragma: no cover
    from local_data_api.resources.resource import ConnectionMaker, Cursor


@register_resource_type
class SQLite(Resource):
    def autocommit_off(self) -> None:
        # default is off
        pass

    def create_column_metadata_set(self, cursor: Cursor) -> List[ColumnMetadata]:
        raise NotImplementedError

    DIALECT = sqlite.dialect(paramstyle='named')

    @classmethod
    def create_connection_maker(
        cls,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user_name: Optional[str] = None,
        password: Optional[str] = None,
        engine_kwargs: Dict[str, Any] = None,
    ) -> ConnectionMaker:
        def connect(_: Optional[str] = None):  # type: ignore
            return sqlite3.connect(':memory:')

        return connect

    def get_field_from_value(self, value: Any) -> Field:
        return super().get_field_from_value(value)
