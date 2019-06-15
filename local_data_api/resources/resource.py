from __future__ import annotations

import random
import string

from abc import ABC
from dataclasses import dataclass

from typing import Optional, Dict, Any, Type
from hashlib import sha1

from sqlalchemy import text
from sqlalchemy.engine import Engine, create_engine, ResultProxy
from sqlalchemy.orm import Session, sessionmaker

from local_data_api.exceptions import BadRequestException, InternalServerErrorException
from local_data_api.secret_manager import get_secret, Secret

TRANSACTION_ID_CHARACTERS: str = string.ascii_letters + '/=+'
TRANSACTION_ID_LENGTH: int = 184

SessionMaker: sessionmaker = sessionmaker()

RESOURCE_CLASS: Dict[str, Type[Resource]] = {}

RESOURCE_METAS: Dict[str, ResourceMeta] = {}

SESSION_POOL: Dict[str, Session] = {}


def set_session(transaction_id: str, session: Session):
    SESSION_POOL[transaction_id] = session


def delete_session(transaction_id: Optional[str]) -> None:
    if transaction_id:
        del SESSION_POOL[transaction_id]


@dataclass
class ResourceMeta:
    resource_type: Type[Resource]
    session_maker: SessionMaker
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
        session_maker=create_session_maker(engine_name, host, port, user_name, password, engine_kwargs),
        host=host, port=port, user_name=user_name, password=password)
    RESOURCE_METAS[resource_arn] = resource_meta


def create_session_maker(engine_name: str, host: Optional[str], port: Optional[int],  user_name: Optional[str] = None,
                         password: Optional[str] = None,
                         engine_kwargs: Optional[Dict[str, Any]] = None) -> SessionMaker:

    resource_class: Type[Resource] = get_resource_class(engine_name)

    engine: Engine = resource_class.create_engine(host, port, user_name, password, engine_kwargs)

    return sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_session(resource_arn: str, **session_kwargs: Any) -> Session:
    return RESOURCE_METAS[resource_arn].session_maker(**session_kwargs)


def get_session(transaction_id: str) -> Session:
    if transaction_id in SESSION_POOL:
        return SESSION_POOL[transaction_id]
    raise BadRequestException('Invalid transaction ID')


def get_resource(resource_arn: str, secret_arn: str, transaction_id: Optional[str] = None) -> Resource:
    if resource_arn not in RESOURCE_METAS:
        if transaction_id in SESSION_POOL:
            raise InternalServerErrorException
        raise BadRequestException(f'HttpEndPoint is not enabled for {resource_arn}')

    try:
        secret: Secret = get_secret(secret_arn)
    except BadRequestException:
        if transaction_id in SESSION_POOL:
            raise InternalServerErrorException
        raise

    meta: ResourceMeta = RESOURCE_METAS[resource_arn]

    # TODO: support multiple secret_arn for a resource
    if secret.user_name != meta.user_name or secret.password != meta.password:
        raise BadRequestException('Invalid secret_arn')

    if transaction_id is None:
        session: Session = create_session(resource_arn, autocommit=True)
    else:
        session = get_session(transaction_id)

    return meta.resource_type(session, transaction_id)


class Resource(ABC):
    DIALECT: str
    DRIVER: Optional[str]

    def __init__(self, session: Session, transaction_id: Optional[str] = None):
        self._session: Session = session
        self._transaction_id: Optional[str] = transaction_id

    @classmethod
    def create_engine(cls, host: Optional[str] = None, port: Optional[int] = None, user_name: Optional[str] = None,
                      password: Optional[str] = None, engine_kwargs: Dict[str, Any] = None) -> Engine:
        url: str = cls.DIALECT
        if cls.DRIVER:
            url += f'+{cls.DRIVER}'
        url += '://'
        if user_name and password:
            url += f'{user_name}:{password}@'
        if host:
            url += host
        if port:
            url += f':{port}'
        if not engine_kwargs:
            engine_kwargs = {}

        return create_engine(url, **engine_kwargs, echo=True)

    @property
    def session(self) -> Session:
        return self._session

    @property
    def transaction_id(self) -> Optional[str]:
        return self._transaction_id

    @staticmethod
    def create_transaction_id() -> str:
        return ''.join(random.choice(TRANSACTION_ID_CHARACTERS) for _ in range(TRANSACTION_ID_LENGTH))

    def close(self) -> None:
        self.session.close()
        delete_session(self.transaction_id)

    def use_database(self, database_name: str) -> None:
        self.execute(f'use {database_name}')

    def begin(self) -> str:
        self.session.begin()
        transaction_id = self.create_transaction_id()
        self._transaction_id = transaction_id
        set_session(transaction_id, self.session)
        return transaction_id

    def commit(self) -> None:
        self.session.commit()
        delete_session(self.transaction_id)

    def rollback(self) -> None:
        self.session.rollback()
        delete_session(self.transaction_id)

    def execute(self, sql: str, params=None, database_name: Optional[str] = None) -> ResultProxy:
        try:
            if database_name:
                self.use_database(database_name)
            return self.session.execute(text(sql), params)
        except Exception as e:
            print(e)
            message: str = 'Unknown'
            if hasattr(e, 'orig') and hasattr(e.orig, 'args'):  # type: ignore
                message = e.orig.args[1]  # type: ignore
            elif len(getattr(e, 'args', [])):
                message = e.args[0]
            raise BadRequestException(message)
