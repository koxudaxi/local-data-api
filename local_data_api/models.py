from __future__ import annotations

from base64 import b64encode
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Schema


class ArrayField(BaseModel):
    blobValues: Optional[List[str]]  # Type: Base64-encoded binary data object
    booleanValues: Optional[List[bool]]
    doubleValues: Optional[List[float]]
    longValues: Optional[List[int]]
    stringValues: Optional[List[str]]  

    @classmethod
    def from_value(cls, values: list) -> Field:
        if len(values) == 0:
            return cls(stringValues=[])
        value = values[0]
        if isinstance(value, bool):
            return cls(booleanValues=values)
        elif isinstance(value, str):
            return cls(stringValues=values)
        elif isinstance(value, int):
            return cls(longValues=values)
        elif isinstance(value, float):
            return cls(doubleValues=values)
        #elif isinstance(value, bytes):
        #    return cls(blobValues=b64encode(values))
        else:
            raise Exception(f'unsupported type {type(value)}: {value} ')

class Field(BaseModel):
    blobValue: Optional[str]  # Type: Base64-encoded binary data object
    booleanValue: Optional[bool]
    doubleValue: Optional[float]
    isNull: Optional[bool]
    longValue: Optional[int]
    stringValue: Optional[str]
    arrayValue: Optional[ArrayField]

    @classmethod
    def from_value(cls, value: Any) -> Field:
        if isinstance(value, bool):
            return cls(booleanValue=value)
        elif isinstance(value, str):
            return cls(stringValue=value)
        elif isinstance(value, int):
            return cls(longValue=value)
        elif isinstance(value, float):
            return cls(doubleValue=value)
        elif isinstance(value, bytes):
            return cls(blobValue=b64encode(value))
        elif isinstance(value, list):
            return cls(arrayValue=ArrayField.from_value(value))
        elif value is None:
            return cls(isNull=True)
        else:
            raise Exception(f'unsupported type {type(value)}: {value} ')


class SqlParameter(BaseModel):
    name: str
    value: Field

    @property
    def valid_value(self: SqlParameter) -> Union[str, bool, float, None, int]:
        for key, value in self.value.dict(skip_defaults=True).items():
            if key == 'isNull' and value:
                return None
            return value
        return None


class SqlParameters(BaseModel):
    __root__: List[SqlParameter]


class ExecuteSqlRequest(BaseModel):
    awsSecretStoreArn: str
    dbClusterOrInstanceArn: str
    sqlStatements: str
    database: Optional[str]
    schema_: Optional[str] = Schema(None, alias='schema')  # type: ignore


class ExecuteStatementRequests(BaseModel):
    resourceArn: str
    secretArn: str
    sql: str
    database: Optional[str]
    continueAfterTimeout: Optional[bool]
    includeResultMetadata: bool = False
    parameters: Optional[SqlParameters]
    schema_: Optional[str] = Schema(None, alias='schema')  # type: ignore
    transactionId: Optional[str]


class ColumnMetadata(BaseModel):
    arrayBaseColumnType: Optional[int]
    isAutoIncrement: Optional[bool]
    isCaseSensitive: Optional[bool]
    isCurrency: Optional[bool]
    isSigned: Optional[bool]
    label: Optional[str]
    name: Optional[str]
    nullable: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    schema_: Optional[str] = Schema(None, alias='schema')  # type: ignore
    tableName: Optional[str]
    type: Optional[int]
    typeName: Optional[str]


class ExecuteStatementResponse(BaseModel):
    numberOfRecordsUpdated: int
    generatedFields: Optional[List[Field]]
    records: Optional[List[List[Field]]]
    columnMetadata: Optional[List[ColumnMetadata]]


class BeginTransactionRequest(BaseModel):
    resourceArn: str
    secretArn: str
    schema_: Optional[str] = Schema(None, alias='schema')  # type: ignore
    database: Optional[str]


class BeginTransactionResponse(BaseModel):
    transactionId: str


class CommitTransactionRequest(BaseModel):
    resourceArn: str
    secretArn: str
    transactionId: str


class TransactionStatus(Enum):
    transaction_committed: str = 'Transaction Committed'
    rollback_complete: str = 'Rollback Complete'


class CommitTransactionResponse(BaseModel):
    transactionStatus: TransactionStatus


class RollbackTransactionRequest(BaseModel):
    resourceArn: str
    secretArn: str
    transactionId: str


class RollbackTransactionResponse(BaseModel):
    transactionStatus: TransactionStatus


class BatchExecuteStatementRequests(BaseModel):
    resourceArn: str
    secretArn: str
    sql: str
    database: Optional[str]
    continueAfterTimeout: Optional[bool]
    includeResultMetadata: Optional[bool]
    parameterSets: Optional[List[SqlParameters]]
    schema_: Optional[str] = Schema(None, alias='schema')  # type: ignore
    transactionId: Optional[str]


class UpdateResult(BaseModel):
    generatedFields: List[Field]


class BatchExecuteStatementResponse(BaseModel):
    updateResults: List[UpdateResult]
