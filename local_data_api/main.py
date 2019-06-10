from typing import List, Dict, Any

from fastapi import FastAPI

from sqlalchemy.engine import ResultProxy
from starlette.requests import Request
from starlette.responses import JSONResponse

from exceptions import DataAPIException
from models import ExecuteSqlRequest, ExecuteStatementRequests, ExecuteStatementResponse, BeginTransactionRequest, \
    BeginTransactionResponse, TransactionStatus, CommitTransactionResponse, CommitTransactionRequest
from database.database import get_database, DataBase, create_database, DUMMY_ARN
from database.mysql import MySQL

app = FastAPI()

# TODO: implement to create custom resource
create_database(MySQL, secret_arn=DUMMY_ARN, resource_arn=DUMMY_ARN)


@app.post("/ExecuteSql")
def execute_sql(request: ExecuteSqlRequest):
    raise NotImplemented


def convert_value(value: Any) -> Dict[str, Any]:
    if isinstance(value, str):
        return {'stringValue': value}
    elif isinstance(value, int):
        return {'longValue': value}
    elif isinstance(value, float):
        return {'doubleValue': value}
    elif value is None:
        return {'isNull': True}
    else:
        raise Exception(f'unsupported type {type(value)}: {value} ')


@app.post("/BeginTransaction")
def begin_statement(request: BeginTransactionRequest) -> BeginTransactionResponse:
    database: DataBase = get_database(request.resourceArn, request.secretArn)
    transaction_id: str = database.begin()

    return BeginTransactionResponse(transactionId=transaction_id)


@app.post("/CommitTransaction")
def commit_transaction(request: CommitTransactionRequest) -> CommitTransactionResponse:
    database: DataBase = get_database(request.resourceArn, request.secretArn)
    database.commit(request.transactionId)

    return CommitTransactionResponse(transactionStatus=TransactionStatus.transaction_committed)


@app.post("/Execute")
def execute_statement(request: ExecuteStatementRequests) -> ExecuteStatementResponse:
    database: DataBase = get_database(request.resourceArn, request.secretArn)

    result: ResultProxy = database.execute(request.sql, request.database, request.transactionId)

    if result.keys():
        records: List[List[Dict[str, Any]]] = [
            [convert_value(column) for column in row]
            for row in result.cursor.fetchall()
        ]
        response: ExecuteStatementResponse = ExecuteStatementResponse(numberOfRecordsUpdated=0,
                                                                      records=records)

    else:
        response = ExecuteStatementResponse(numberOfRecordsUpdated=result.rowcount,
                                            generatedFields=[])

    if request.includeResultMetadata:
        response.columnMetadata = []

    return response


@app.exception_handler(DataAPIException)
async def data_api_exception_handler(_: Request, exc: DataAPIException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"message": exc.message, "code": exc.code},
    )
