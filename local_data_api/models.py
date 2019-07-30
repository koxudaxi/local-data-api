from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Schema


class Field(BaseModel):
    blobValue: Optional[str]  # Type: Base64-encoded binary data object
    booleanValue: Optional[bool]
    doubleValue: Optional[float]
    isNull: Optional[bool]
    longValue: Optional[int]
    stringValue: Optional[str]


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
    includeResultMetadata: Optional[bool]
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
    records: Optional[List[List[Dict[str, Any]]]]
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
