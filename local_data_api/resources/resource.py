from __future__ import annotations

import random
import re
import string
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from hashlib import sha1
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Pattern, Tuple, Type, Union

from pydantic.main import BaseModel
from sqlalchemy import text
from sqlalchemy.engine import Dialect
from sqlalchemy.exc import ArgumentError, CompileError
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.expression import null

from local_data_api.exceptions import BadRequestException, InternalServerErrorException
from local_data_api.models import ColumnMetadata, ExecuteStatementResponse, Field
from local_data_api.secret_manager import Secret, get_secret

INVALID_PARAMETER_MESSAGE: str = r"Bind parameter '([^\']+)' without a renderable value not allowed here."
UNDEFINED_PARAMETER_MESSAGE: str = r"This text\(\) construct doesn't define a bound parameter named '([^\']+)'"

TRANSACTION_ID_CHARACTERS: str = string.ascii_letters + '/=+'
TRANSACTION_ID_LENGTH: int = 184

RESOURCE_CLASS: Dict[str, Type[Resource]] = {}

RESOURCE_METAS: Dict[str, ResourceMeta] = {}

CONNECTION_POOL: Dict[str, Connection] = {}

# DBAPI's Types
if TYPE_CHECKING:  # pragma: no cover
    from typing import Callable

    connect = Callable

    class Connection:
        database: Optional[str] = None

        def close(self) -> None:
            pass

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def cursor(self) -> Cursor:
            return Cursor()

        @property
        def jconn(self) -> Jconn:
            pass

        def get_dsn_parameters(self) -> Dict[str, str]:
            pass

    class Jconn:
        def setAutoCommit(self, flag: bool) -> None:
            pass

        def setCatalog(self, catalog: str) -> None:
            pass

    class Cursor:
        def execute(self, *args: Any, **kwargs: Any) -> Any:
            pass

        def fetchone(self) -> Tuple:
            pass

        def fetchmany(self, _: Any) -> Tuple[Tuple]:
            pass

        def fetchall(self) -> Tuple[Tuple]:
            pass

        def close(self) -> None:
            pass

        @property
        def description(self) -> Tuple[Tuple]:
            return ((),)

    ConnectionMaker = Callable[[Optional[str]], Connection]


def set_connection(transaction_id: str, connection: Connection) -> None:
    CONNECTION_POOL[transaction_id] = connection


def delete_connection(transaction_id: str) -> None:
    del CONNECTION_POOL[transaction_id]


@dataclass
class ResourceMeta:
    resource_type: Type[Resource]
    connection_maker: ConnectionMaker
    host: Optional[str] = None
    port: Optional[int] = None
    user_name: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None


def register_resource_type(resource: Type[Resource]) -> Type[Resource]:
    RESOURCE_CLASS[resource.__name__] = resource
    return resource


def get_resource_class(engine_name: str) -> Type[Resource]:
    try:
        return RESOURCE_CLASS[engine_name]
    except KeyError:
        raise Exception(f'Invalid engine name: {engine_name}')


def create_resource_arn(
    region_name: str = 'us-east-1', account: str = '123456789012'
) -> str:
    return f'arn:aws:rds:{region_name}:{account}:cluster:local-data-api{sha1().hexdigest()}'


def register_resource(
    resource_arn: str,
    engine_name: str,
    host: Optional[str],
    port: Optional[int],
    user_name: Optional[str] = None,
    password: Optional[str] = None,
    engine_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    resource_meta = ResourceMeta(
        resource_type=get_resource_class(engine_name),
        connection_maker=create_connection_maker(
            engine_name, host, port, user_name, password, engine_kwargs
        ),
        host=host,
        port=port,
        user_name=user_name,
        password=password,
    )
    RESOURCE_METAS[resource_arn] = resource_meta


def create_connection_maker(
    engine_name: str,
    host: Optional[str],
    port: Optional[int],
    user_name: Optional[str] = None,
    password: Optional[str] = None,
    engine_kwargs: Optional[Dict[str, Any]] = None,
) -> ConnectionMaker:
    resource_class: Type[Resource] = get_resource_class(engine_name)

    return resource_class.create_connection_maker(
        host, port, user_name, password, engine_kwargs
    )


def create_connection(
    resource_arn: str, database: Optional[str] = None, **connection_kwargs: Any
) -> Connection:
    connection = RESOURCE_METAS[resource_arn].connection_maker(  # type: ignore
        database, **connection_kwargs
    )
    if hasattr(connection, 'database'):  # pragma: no cover
        connection.database = database
    return connection


def get_connection(transaction_id: str) -> Connection:
    if transaction_id in CONNECTION_POOL:
        return CONNECTION_POOL[transaction_id]
    raise BadRequestException('Invalid transaction ID')


def get_resource(
    resource_arn: str,
    secret_arn: str,
    transaction_id: Optional[str] = None,
    database: Optional[str] = None,
) -> Resource:
    if resource_arn not in RESOURCE_METAS:
        if transaction_id in CONNECTION_POOL:
            raise InternalServerErrorException
        raise BadRequestException(f'HttpEndPoint is not enabled for {resource_arn}')

    try:
        secret: Secret = get_secret(secret_arn)
    except BadRequestException:
        if transaction_id in CONNECTION_POOL:
            raise InternalServerErrorException
        raise

    meta: ResourceMeta = RESOURCE_METAS[resource_arn]

    # TODO: support multiple secret_arn for a resource
    if secret.user_name != meta.user_name or secret.password != meta.password:
        raise BadRequestException('Invalid secret_arn')

    if transaction_id is None:
        connection: Connection = create_connection(resource_arn, database)
    else:
        connection = get_connection(transaction_id)
        if database:
            if hasattr(connection, 'database'):  # pragma: no cover
                connected_database: Optional[str] = connection.database
            else:
                connected_database = connection.get_dsn_parameters()[
                    'dbname'
                ]  # pragma: no cover
            if database != connected_database:  # pragma: no cover
                raise BadRequestException(
                    'Database name is not the same as when transaction was created'
                )

    return meta.resource_type(connection, transaction_id)


class JDBCType(Enum):
    BIT = -7
    TINYINT = -6
    SMALLINT = 5
    INTEGER = 4
    BIGINT = -5
    FLOAT = 6
    REAL = 7
    DOUBLE = 8
    NUMERIC = 2
    DECIMAL = 3
    CHAR = 1
    VARCHAR = 12
    LONGVARCHAR = -1
    DATE = 91
    TIME = 92
    TIMESTAMP = 93
    BINARY = -2
    VARBINARY = -3
    LONGVARBINARY = -4
    NULL = 0
    OTHER = 1111
    JAVA_OBJECT = 2000
    DISTINCT = 2001
    STRUCT = 2002
    ARRAY = 2003
    BLOB = 2004
    CLOB = 2005
    REF = 2006
    DATALINK = 70
    BOOLEAN = 16
    ROWID = -8
    NCHAR = -15
    NVARCHAR = -9
    LONGNVARCHAR = -16
    NCLOB = 2011
    SQLXML = 2009
    REF_CURSOR = 2012
    TIME_WITH_TIMEZONE = 2013
    TIMESTAMP_WITH_TIMEZONE = 2014


class Resource(ABC):
    DIALECT: Dialect

    def __init__(self, connection: Connection, transaction_id: Optional[str] = None):
        self._connection: Connection = connection
        self._transaction_id: Optional[str] = transaction_id

    @classmethod
    def create_query(cls, sql: str, params: Dict[str, Any]) -> str:
        text_sql: TextClause = text(sql)
        kwargs = {'dialect': cls.DIALECT, 'compile_kwargs': {"literal_binds": True}}
        try:
            return str(
                text_sql.bindparams(
                    **{k: null() if v is None else v for k, v in params.items()}
                ).compile(**kwargs)
            )
        except CompileError as e:
            invalid_param_match = re.match(INVALID_PARAMETER_MESSAGE, e.args[0])
            if invalid_param_match:  # pragma: no cover
                raise BadRequestException(
                    message=f'Cannot find parameter: {invalid_param_match.group(1)}'
                )
            raise  # pragma: no cover
        except ArgumentError as e:
            undefined_param_match = re.match(UNDEFINED_PARAMETER_MESSAGE, e.args[0])
            if undefined_param_match:  # pragma: no cover
                undefined_param: str = undefined_param_match.group(1)
                return cls.create_query(
                    sql, {k: v for k, v in params.items() if k != undefined_param}
                )
            raise  # pragma: no cover

    @classmethod
    @abstractmethod
    def create_connection_maker(
        cls,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user_name: Optional[str] = None,
        password: Optional[str] = None,
        engine_kwargs: Dict[str, Any] = None,
    ) -> ConnectionMaker:
        raise NotImplementedError

    @property
    def connection(self) -> Connection:
        return self._connection

    @property
    def transaction_id(self) -> Optional[str]:
        return self._transaction_id

    @staticmethod
    def create_transaction_id() -> str:
        return ''.join(
            random.choice(TRANSACTION_ID_CHARACTERS)
            for _ in range(TRANSACTION_ID_LENGTH)
        )

    @abstractmethod
    def create_column_metadata_set(self, cursor: Cursor) -> List[ColumnMetadata]:
        raise NotImplementedError

    def close(self) -> None:
        self.connection.close()
        if self.transaction_id in CONNECTION_POOL:
            delete_connection(self.transaction_id)

    def begin(self) -> str:
        transaction_id = self.create_transaction_id()
        self._transaction_id = transaction_id
        set_connection(transaction_id, self.connection)
        return transaction_id

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def execute(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        include_result_metadata: bool = False,
    ) -> ExecuteStatementResponse:

        try:
            cursor: Optional[Cursor] = None
            try:
                cursor = self.connection.cursor()
                if params:
                    cursor.execute(self.create_query(sql, params))
                else:
                    cursor.execute(str(text(sql)))

                if cursor.description:
                    response: ExecuteStatementResponse = ExecuteStatementResponse(
                        numberOfRecordsUpdated=0,
                        records=[
                            [Field.from_value(column) for column in row]
                            for row in cursor.fetchall()
                        ],
                    )
                    if include_result_metadata:
                        response.columnMetadata = self.create_column_metadata_set(
                            cursor
                        )
                    return response
                else:
                    rowcount: int = cursor.rowcount
                    last_generated_id: int = cursor.lastrowid
                    generated_fields: List[Field] = []
                    if last_generated_id > 0:
                        generated_fields.append(Field.from_value(last_generated_id))
                    return ExecuteStatementResponse(
                        numberOfRecordsUpdated=rowcount,
                        generatedFields=generated_fields,
                    )
            finally:
                if cursor:  # pragma: no cover
                    cursor.close()

        except Exception as e:
            message: str = 'Unknown'
            if hasattr(e, 'orig') and hasattr(e.orig, 'args'):  # type: ignore
                message = str(e.orig.args[1])  # type: ignore
            elif len(getattr(e, 'args', [])) and e.args[0]:
                message = str(e.args[0])
            raise BadRequestException(message)
