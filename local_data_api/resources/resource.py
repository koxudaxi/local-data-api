from __future__ import annotations

import random
import string


from abc import ABC

from typing import Optional, Dict, Any, Type
from hashlib import sha1

from sqlalchemy.engine import Engine, create_engine, ResultProxy
from sqlalchemy.orm import Session, sessionmaker

from local_data_api.exceptions import BadRequestException
from local_data_api.secret_manager import get_secret, Secret

TRANSACTION_ID_CHARACTERS: str = string.ascii_letters + '/=+'
TRANSACTION_ID_LENGTH: int = 184

_Session = sessionmaker()

RESOURCE_CLASS: Dict[str, Type[Resource]] = {}

RESOURCE_POOL: Dict[str, Resource] = {}


def register_resource(resource: Type[Resource]) -> Type[Resource]:
    RESOURCE_CLASS[resource.__name__] = resource
    return resource


def get_resource_class(engine_name: str):
    try:
        return RESOURCE_CLASS[engine_name]
    except KeyError:
        raise Exception(f'Invalid engine name: {engine_name}')


def create_resource_arn(region_name: str = 'us-east-1', account: str = '123456789012') -> str:
    return f'arn:aws:rds:{region_name}:{account}:cluster:local-data-api{sha1().hexdigest()}'


def create_resource(engine_name: str, host: str, port: int,  user_name: Optional[str] = None,
                    password: Optional[str] = None, resource_arn: Optional[str] = None,
                    engine_kwargs: Optional[Dict[str, Any]] = None) -> str:
    if not resource_arn:
        resource_arn = create_resource_arn()

    resource_class: Type[Resource] = get_resource_class(engine_name)
    RESOURCE_POOL[resource_arn] = resource_class(host, port, user_name, password, engine_kwargs)

    return resource_arn


def get_resource(resource_arn: str, secret_arn: str) -> Resource:
    if resource_arn not in RESOURCE_POOL:
        raise BadRequestException(f'HttpEndPoint is not enabled for {resource_arn}')

    resource = RESOURCE_POOL[resource_arn]

    secret: Secret = get_secret(secret_arn)
    # TODO: support multiple secret_arn for a resource
    if secret.user_name != resource.user_name or secret.password != resource.password:
        raise BadRequestException('Invalid secret_arn')

    return resource


class Resource(ABC):
    DIALECT: str
    DRIVER: Optional[str]

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 user_name: Optional[str] = None, password: Optional[str] = None,
                 engine_kwargs: Optional[Dict[str, Any]] = None):
        self.host: Optional[str] = host
        self.port: Optional[int] = port
        self.user_name: Optional[str] = user_name
        self.password: Optional[str] = password
        self.engine_kwargs: Dict[str, Any] = engine_kwargs or {}
        self.engine: Engine = self.get_engine()
        self.sessions: Dict[str, Session] = {}

    def get_engine(self) -> Engine:
        url: str = self.DIALECT
        if self.DRIVER:
            url += f'+{self.DRIVER}'
        url += '://'
        if self.user_name and self.password:
            url += f'{self.user_name}:{self.password}@'
        if self.host:
            url += self.host
        if self.port:
            url += f':{self.port}'

        return create_engine(url, **self.engine_kwargs)

    @staticmethod
    def create_transaction_id() -> str:
        return ''.join(random.choice(TRANSACTION_ID_CHARACTERS) for _ in range(TRANSACTION_ID_LENGTH))

    def use_database(self, database_name: str) -> None:
        self.execute(f'use {database_name}')

    def begin(self) -> str:
        transaction_id: str = self.create_transaction_id()
        self.sessions[transaction_id] = _Session(bind=self.engine, autocommit=False, autoflush=True)
        return transaction_id

    def commit(self, transaction_id: str) -> None:
        session: Session = self.get_session(transaction_id)
        session.commit()
        session.close()

    def rollback(self, transaction_id: str) -> None:
        session: Session = self.get_session(transaction_id)
        session.rollback()
        session.close()

    def execute(self, sql: str, database_name: Optional[str] = None,
                transaction_id: Optional[str] = None) -> ResultProxy:
        try:
            if database_name:
                self.use_database(database_name)
            session: Session = self.get_session(transaction_id)
            return session.execute(sql.rstrip('; '))
        except Exception as e:
            message: str = 'Unknown'
            if hasattr(e, 'orig') and hasattr(e.orig, 'args'):  # type: ignore
                message = e.orig.args[1]  # type: ignore
            elif len(getattr(e, 'args', [])):
                message = e.args[0]
            raise BadRequestException(message)

    def get_session(self, transaction_id: Optional[str] = None) -> Session:
        if transaction_id:
            if transaction_id in self.sessions:
                return self.sessions[transaction_id]
            raise Exception(f'{transaction_id} is not found')
        return _Session(bind=self.engine, autocommit=True, autoflush=True)
