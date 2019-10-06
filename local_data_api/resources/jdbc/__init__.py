from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import jaydebeapi
from local_data_api.models import ColumnMetadata
from local_data_api.resources.resource import Resource
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Dialect

if TYPE_CHECKING:  # pragma: no cover
    from local_data_api.resources.resource import ConnectionMaker, Connection


def attach_thread_to_jvm() -> None:
    "https://github.com/baztian/jaydebeapi/issues/14#issuecomment-261489331"

    import jpype

    if jpype.isJVMStarted() and not jpype.isThreadAttachedToJVM():
        jpype.attachThreadToJVM()
        jpype.java.lang.Thread.currentThread().setContextClassLoader(
            jpype.java.lang.ClassLoader.getSystemClassLoader()
        )


def connection_maker(
    jclassname: str,
    url: str,
    driver_args: Union[Dict, List] = None,
    jars: Union[List[str], str] = None,
    libs: Union[List[str], str] = None,
) -> ConnectionMaker:
    def connect(**kwargs):  # type: ignore
        attach_thread_to_jvm()
        return jaydebeapi.connect(jclassname, url, driver_args, jars, libs)

    return connect


class JDBC(Resource, ABC):
    JDBC_NAME: str
    DRIVER: str
    DIALECT: Dialect = mysql.dialect(paramstyle='named')

    def __init__(self, connection: Connection, transaction_id: Optional[str] = None):
        if transaction_id:
            attach_thread_to_jvm()

        super().__init__(connection, transaction_id)

    @classmethod
    def create_connection_maker(
        cls,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user_name: Optional[str] = None,
        password: Optional[str] = None,
        engine_kwargs: Dict[str, Any] = None,
    ) -> ConnectionMaker:

        url: str = f'{cls.JDBC_NAME}://{host}:{port}'

        if not engine_kwargs or 'JAR_PATH' not in engine_kwargs:
            raise Exception('Not Found JAR_PATH in settings')

        return connection_maker(
            cls.DRIVER,
            url,
            {"user": user_name, "password": password},
            engine_kwargs['JAR_PATH'],
        )


def create_column_metadata_set(meta: Any) -> List[ColumnMetadata]:
    return [
        ColumnMetadata(
            arrayBaseColumnType=0,
            isAutoIncrement=meta.isAutoIncrement(i),
            isCaseSensitive=meta.isCaseSensitive(i),
            isCurrency=meta.isCurrency(i),
            isSigned=meta.isSigned(i),
            label=meta.getColumnLabel(i),
            name=meta.getColumnName(i),
            nullable=meta.isNullable(i),
            precision=meta.getPrecision(i),
            scale=meta.getScale(i),
            schema=meta.getSchemaName(i),
            tableName=meta.getTableName(i),
            type=meta.getColumnType(i),
            typeName=meta.getColumnTypeName(i),
        )
        for i in range(1, meta.getColumnCount() + 1)
    ]
