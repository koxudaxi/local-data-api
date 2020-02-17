from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING, Any, Dict, Optional

from local_data_api.resources.resource import Resource, register_resource_type
from sqlalchemy.dialects import sqlite

if TYPE_CHECKING:  # pragma: no cover
    from local_data_api.resources.resource import ConnectionMaker


@register_resource_type
class SQLite(Resource):
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
