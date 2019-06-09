from __future__ import annotations

import random
import string


from abc import ABC
from dataclasses import dataclass
from typing import Optional, Dict, Any, Type, Union
from sqlalchemy.engine import Engine, create_engine, ResultProxy
from sqlalchemy.orm import Session, sessionmaker

from settings import DATABASE_SETTINGS

_Session = sessionmaker()

DATABASE_POOL: Dict[str, DataBaseMeta] = {}

TRANSACTION_ID_CHARACTERS: str = string.ascii_letters + '/=+'
TRANSACTION_ID_LENGTH: int = 184

DUMMY_ARN: str = 'dummy'


@dataclass
class DataBaseMeta:
    resource_arn: str
    secret_arn: str
    database: DataBase


def create_database(database_type: Type[DataBase], resource_arn: str, secret_arn: str) -> None:
    setting: Dict[str, Union[str, int]] = DATABASE_SETTINGS[database_type.__name__]
    database: DataBase = database_type(**setting)
    DATABASE_POOL[resource_arn] = DataBaseMeta(resource_arn=resource_arn, secret_arn=secret_arn, database=database)


def get_database(resource_arn: str, secret_arn: str) -> DataBase:
    if resource_arn not in DATABASE_POOL:
        raise Exception('Not Found DatabaseMeta')

    database_meta: DataBaseMeta = DATABASE_POOL[resource_arn]

    if database_meta.secret_arn != secret_arn:
        raise Exception('Invalid secretArn')

    return database_meta.database


class DataBase(ABC):
    DIALECT: str
    DRIVER: str

    def __init__(self, host: str, port: int, user_name: str, password: str,
                 engine_kwargs: Optional[Dict[str, Any]] = None):
        self.host: str = host
        self.port: int = port
        self.user_name: str = user_name
        self.password: str = password
        self.engine_kwargs = engine_kwargs or {}
        self.engine: Engine = self.get_engine()
        self.sessions: Dict[str, Session] = {}

    def get_engine(self) -> Engine:
        url: str = f'{self.DIALECT}+{self.DRIVER}://{self.user_name}:{self.password}@{self.host}:{self.port}'
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
        if database_name:
            self.use_database(database_name)
        session: Session = self.get_session(transaction_id)
        return session.execute(sql.rstrip('; '))

    def get_session(self, transaction_id: Optional[str] = None) -> Session:
        if transaction_id:
            if transaction_id in self.sessions:
                return self.sessions[transaction_id]
            raise Exception(f'{transaction_id} is not found')
        return _Session(bind=self.engine, autocommit=True, autoflush=True)
