from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

import psycopg2
from sqlalchemy.dialects import postgresql

from local_data_api.models import ColumnMetadata
from local_data_api.resources.resource import Resource, register_resource_type

if TYPE_CHECKING:  # pragma: no cover
    from local_data_api.resources.resource import ConnectionMaker, Cursor


@register_resource_type
class PostgresSQL(Resource):
    def create_column_metadata_set(
        self, cursor: Cursor
    ) -> List[ColumnMetadata]:  # pragma: no cover
        raise NotImplementedError

    DIALECT = postgresql.dialect(paramstyle='named')

    @classmethod
    def create_connection_maker(
        cls,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user_name: Optional[str] = None,
        password: Optional[str] = None,
        engine_kwargs: Dict[str, Any] = None,
    ) -> ConnectionMaker:
        kwargs: Dict[str, Any] = {}
        if host:
            kwargs['host'] = host
        if port:
            kwargs['port'] = port
        if user_name:
            kwargs['user'] = user_name
        if password:
            kwargs['password'] = password
        if engine_kwargs:
            kwargs.update(engine_kwargs)

        def connect(database: Optional[str] = None):  # type: ignore
            if database:
                kwargs['dbname'] = database
            return psycopg2.connect(**kwargs)

        return connect
