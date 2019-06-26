from __future__ import annotations

from typing import Optional, Dict, Any, TYPE_CHECKING

from sqlalchemy.dialects import mysql

from local_data_api.resources.resource import Resource, register_resource_type

import pymysql

if TYPE_CHECKING:
    from local_data_api.resources.resource import ConnectionMaker


@register_resource_type
class MySQL(Resource):
    DIALECT = mysql.dialect(paramstyle='named')

    @classmethod
    def create_connection_maker(cls, host: Optional[str] = None, port: Optional[int] = None,
                                user_name: Optional[str] = None, password: Optional[str] = None,
                                engine_kwargs: Dict[str, Any] = None) -> ConnectionMaker:
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

        def connect():
            return pymysql.connect(**kwargs)

        return connect
