from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pymysql
from pymysql import protocol
from pymysql.constants import FIELD_TYPE
from pymysql.protocol import FieldDescriptorPacket
from sqlalchemy.dialects import mysql

from local_data_api.models import ColumnMetadata
from local_data_api.resources.resource import Resource, register_resource_type

if TYPE_CHECKING:  # pragma: no cover
    from local_data_api.resources.resource import ConnectionMaker, Cursor

FIELD_TYPE_MAP: Dict[int, str] = {
    getattr(FIELD_TYPE, k): k for k in dir(FIELD_TYPE) if not k.startswith('_')
}


def create_column_metadata(
    field_descriptor_packet: FieldDescriptorPacket,
) -> ColumnMetadata:
    return ColumnMetadata(
        arrayBaseColumnType=0,
        isAutoIncrement=False,
        isCaseSensitive=False,
        isCurrency=False,
        isSigned=False,
        label=field_descriptor_packet.name,
        name=field_descriptor_packet.org_name,
        nullable=1 if field_descriptor_packet.flags % 2 == 0 else 0,
        precision=field_descriptor_packet.get_column_length(),
        scale=field_descriptor_packet.scale,
        schema=None,
        tableName=field_descriptor_packet.table_name,
        type=None,  # JDBC Type unsupported
        typeName=None,  # JDBC TypeName unsupported
    )


@register_resource_type
class MySQL(Resource):
    def create_column_metadata_set(self, cursor: Cursor) -> List[ColumnMetadata]:
        return [create_column_metadata(f) for f in getattr(cursor, '_result').fields]

    DIALECT = mysql.dialect(paramstyle='named')

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
                kwargs['db'] = database
            return pymysql.connect(**kwargs)

        return connect
