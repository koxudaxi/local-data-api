from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import JSONResponse

from local_data_api.exceptions import DataAPIException
from local_data_api.models import BatchExecuteStatementRequests, BatchExecuteStatementResponse, \
    BeginTransactionRequest, BeginTransactionResponse, CommitTransactionRequest, CommitTransactionResponse, \
    ExecuteSqlRequest, ExecuteStatementRequests, ExecuteStatementResponse, RollbackTransactionRequest, \
    RollbackTransactionResponse, TransactionStatus, UpdateResult
from local_data_api.resources.resource import Resource, get_resource
from local_data_api.settings import setup

app = FastAPI()

setup()


@app.post("/ExecuteSql")
def execute_sql(request: ExecuteSqlRequest):
    raise NotImplementedError


@app.post("/BeginTransaction")
def begin_statement(request: BeginTransactionRequest) -> BeginTransactionResponse:
    resource: Resource = get_resource(request.resourceArn, request.secretArn)
    transaction_id: str = resource.begin()

    return BeginTransactionResponse(transactionId=transaction_id)


@app.post("/CommitTransaction")
def commit_transaction(request: CommitTransactionRequest) -> CommitTransactionResponse:
    resource: Resource = get_resource(request.resourceArn, request.secretArn, request.transactionId)
    resource.commit()
    resource.close()
    return CommitTransactionResponse(transactionStatus=TransactionStatus.transaction_committed)


@app.post("/RollbackTransaction")
def rollback_transaction(request: RollbackTransactionRequest) -> RollbackTransactionResponse:
    resource: Resource = get_resource(request.resourceArn, request.secretArn, request.transactionId)
    resource.rollback()
    resource.close()

    return RollbackTransactionResponse(transactionStatus=TransactionStatus.rollback_complete)


@app.post("/Execute")
def execute_statement(request: ExecuteStatementRequests) -> ExecuteStatementResponse:
    resource: Optional[Resource] = None
    try:
        resource = get_resource(request.resourceArn, request.secretArn, request.transactionId)

        if request.parameters:
            parameters: Optional[Dict[str, Any]] = {parameter.name: parameter.valid_value
                                                    for parameter in request.parameters
                                                    }
        else:
            parameters = None

        response: ExecuteStatementResponse = resource.execute(request.sql, parameters, request.database)

        if request.includeResultMetadata:
            response.columnMetadata = []

        if not resource.transaction_id:
            resource.commit()
        return response
    finally:
        if resource and not resource.transaction_id:
            resource.close()


@app.post("/BatchExecute")
def batch_execute_statement(request: BatchExecuteStatementRequests) -> BatchExecuteStatementResponse:
    resource: Optional[Resource] = None
    try:
        resource = get_resource(request.resourceArn, request.secretArn, request.transactionId)

        update_results: List[UpdateResult] = []

        if not request.parameterSets:
            response: BatchExecuteStatementResponse = BatchExecuteStatementResponse(updateResults=update_results)
        else:
            for parameter_set in request.parameterSets:
                parameters: Dict[str, Any] = {parameter.name: parameter.valid_value for parameter in parameter_set}
                result: ExecuteStatementResponse = resource.execute(request.sql, parameters, request.database)

                update_results.append(UpdateResult(generatedFields=result.generatedFields or []))

            response = BatchExecuteStatementResponse(updateResults=update_results)

        if not resource.transaction_id:
            resource.commit()
        return response
    finally:
        if resource and not resource.transaction_id:
            resource.close()


@app.exception_handler(DataAPIException)
async def data_api_exception_handler(_: Request, exc: DataAPIException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"message": exc.message, "code": exc.code},
    )
