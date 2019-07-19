from __future__ import annotations

import random
import string
from abc import ABC, abstractmethod
from dataclasses import dataclass
from hashlib import sha1
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, Type

from sqlalchemy import text
from sqlalchemy.engine import Dialect
from sqlalchemy.sql.elements import TextClause

from local_data_api import convert_value
from local_data_api.exceptions import BadRequestException, InternalServerErrorException
from local_data_api.models import ColumnMetadata, ExecuteStatementResponse
from local_data_api.secret_manager import Secret, get_secret

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
        def close(self):
            pass

        def commit(self):
            pass

        def rollback(self):
            pass

        def cursor(self) -> Cursor:
            return Cursor()

    class Cursor:
        def execute(self, *args, **kwargs):
            pass

        def fetchone(self) -> Tuple:
            pass

        def fetchmany(self, _) -> Tuple[Tuple]:
            pass

        def fetchall(self) -> Tuple[Tuple]:
            pass

        def close(self):
            pass

        @property
        def description(self) -> Tuple[Tuple]:
            return ((),)

    ConnectionMaker = Callable[..., Connection]


def set_connection(transaction_id: str, connection: Connection):
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


def register_resource_type(resource: Type[Resource]) -> Type[Resource]:
    RESOURCE_CLASS[resource.__name__] = resource
    return resource


def get_resource_class(engine_name: str):
    try:
        return RESOURCE_CLASS[engine_name]
    except KeyError:
        raise Exception(f'Invalid engine name: {engine_name}')


def create_resource_arn(region_name: str = 'us-east-1', account: str = '123456789012') -> str:
    return f'arn:aws:rds:{region_name}:{account}:cluster:local-data-api{sha1().hexdigest()}'


def register_resource(resource_arn: str, engine_name: str, host: Optional[str], port: Optional[int],
                      user_name: Optional[str] = None, password: Optional[str] = None,
                      engine_kwargs: Optional[Dict[str, Any]] = None) -> None:
    resource_meta = ResourceMeta(
        resource_type=get_resource_class(engine_name),
        connection_maker=create_connection_maker(engine_name, host, port, user_name, password, engine_kwargs),
        host=host, port=port, user_name=user_name, password=password)
    RESOURCE_METAS[resource_arn] = resource_meta


def create_connection_maker(engine_name: str, host: Optional[str], port: Optional[int],
                            user_name: Optional[str] = None, password: Optional[str] = None,
                            engine_kwargs: Optional[Dict[str, Any]] = None) -> ConnectionMaker:
    resource_class: Type[Resource] = get_resource_class(engine_name)

    return resource_class.create_connection_maker(host, port, user_name, password, engine_kwargs)


def create_connection(resource_arn: str, **connection_kwargs: Any) -> Connection:
    return RESOURCE_METAS[resource_arn].connection_maker(**connection_kwargs)


def get_connection(transaction_id: str) -> Connection:
    if transaction_id in CONNECTION_POOL:
        return CONNECTION_POOL[transaction_id]
    raise BadRequestException('Invalid transaction ID')


def get_resource(resource_arn: str, secret_arn: str, transaction_id: Optional[str] = None) -> Resource:
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
        connection: Connection = create_connection(resource_arn)
    else:
        connection = get_connection(transaction_id)

    return meta.resource_type(connection, transaction_id)


def create_column_metadata(name: str, type_code: int, display_size: Optional[int], internal_size: int, precision: int,
                           scale: int, null_ok: bool) -> ColumnMetadata:
    return ColumnMetadata(arrayBaseColumnType=0,
                          isAutoIncrement=False,
                          isCaseSensitive=False,
                          isCurrency=False,
                          isSigned=False,
                          label=name,
                          name=name,
                          nullabl=1 if null_ok else 0,
                          precision=precision,
                          scale=scale,
                          schema_=None,
                          tableName=None,
                          type=None,
                          typeName=None
                          )


class Resource(ABC):
    DIALECT: Dialect

    def __init__(self, connection: Connection, transaction_id: Optional[str] = None):
        self._connection: Connection = connection
        self._transaction_id: Optional[str] = transaction_id

    @classmethod
    def create_query(cls, sql: str, params: Dict[str, Any]) -> str:
        text_sql: TextClause = text(sql)
        kwargs = {'dialect': cls.DIALECT, 'compile_kwargs': {"literal_binds": True}}
        return str(text_sql.bindparams(**params).compile(**kwargs))

    @classmethod
    @abstractmethod
    def create_connection_maker(cls, host: Optional[str] = None, port: Optional[int] = None,
                                user_name: Optional[str] = None, password: Optional[str] = None,
                                engine_kwargs: Dict[str, Any] = None) -> ConnectionMaker:
        raise NotImplementedError

    @property
    def connection(self) -> Connection:
        return self._connection

    @property
    def transaction_id(self) -> Optional[str]:
        return self._transaction_id

    @staticmethod
    def create_transaction_id() -> str:
        return ''.join(random.choice(TRANSACTION_ID_CHARACTERS) for _ in range(TRANSACTION_ID_LENGTH))

    def close(self) -> None:
        self.connection.close()
        if self.transaction_id in CONNECTION_POOL:
            delete_connection(self.transaction_id)

    def use_database(self, database_name: str) -> None:
        self.execute(f'use {database_name}')

    def begin(self) -> str:
        transaction_id = self.create_transaction_id()
        self._transaction_id = transaction_id
        set_connection(transaction_id, self.connection)
        return transaction_id

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None,
                database_name: Optional[str] = None,
                include_result_metadata: bool = False) -> ExecuteStatementResponse:

        try:
            if database_name:
                self.use_database(database_name)

            cursor: Optional[Cursor] = None
            try:
                cursor = self.connection.cursor()
                if params:
                    cursor.execute(self.create_query(sql, params))
                else:
                    cursor.execute(str(text(sql)))

                if cursor.description:
                    response: ExecuteStatementResponse = ExecuteStatementResponse(numberOfRecordsUpdated=0,
                                                                                  records=[
                                                                                      [convert_value(column) for column
                                                                                       in row]
                                                                                      for row in cursor.fetchall()
                                                                                  ])
                    if include_result_metadata:
                        response.columnMetadata = [create_column_metadata(*d) for d in cursor.description]
                    return response
                else:
                    rowcount: int = cursor.rowcount
                    last_generated_id: int = cursor.lastrowid
                    generated_fields: List[Dict[str, Any]] = []
                    if last_generated_id > 0:
                        generated_fields.append(convert_value(last_generated_id))
                    return ExecuteStatementResponse(numberOfRecordsUpdated=rowcount,
                                                    generatedFields=generated_fields)
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
