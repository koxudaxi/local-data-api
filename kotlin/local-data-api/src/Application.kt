package com.koxudaxi.local_data_api

import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.request.*
import io.ktor.routing.*
import io.ktor.serialization.*
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

const val TRANSACTION_COMMITTED = "Transaction Committed"
const val ROLLBACK_COMPLETE = "Rollback Complete"

@Suppress("unused") // Referenced in application.conf
@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {
    install(ContentNegotiation) {
        json(DefaultJson, ContentType.Any)
    }
    install(StatusPages) {
        exception<SQLException> { cause ->
            val badRequestException = BadRequestException.fromSQLException(cause)
            call.respond(badRequestException.statusCode,
                ErrorResponse(badRequestException.message, badRequestException.code)
            )
        }
        exception<DataAPIException> { cause ->
            call.respond(cause.statusCode, ErrorResponse(cause.message, cause.code))
        }
    }
    routing {

        val resourceManager = ResourceManager.INSTANCE
        setup()

        post("/ExecuteSql") {
            throw NotImplementedError()
        }
        post("/BeginTransaction") {
            val request = call.receive<BeginTransactionRequest>()
            val resource = resourceManager.getResource(request.resourceArn, request.secretArn, null, request.database)
            val transactionId = resource.begin()
            call.respond(BeginTransactionResponse(transactionId))
        }
        post("/CommitTransaction") {
            val request = call.receive<CommitTransactionRequest>()
            val resource =
                resourceManager.getResource(request.resourceArn, request.secretArn, request.transactionId, null)
            resource.commit()
            resource.close()
            call.respond(CommitTransactionResponse(TRANSACTION_COMMITTED))
        }
        post("/RollbackTransaction") {
            val request = call.receive<RollbackTransactionRequest>()
            val resource =
                resourceManager.getResource(request.resourceArn, request.secretArn, request.transactionId, null)
            resource.rollback()
            resource.close()
            call.respond(RollbackTransactionResponse(ROLLBACK_COMPLETE))
        }
        post("/Execute") {
            val request = call.receive<ExecuteStatementRequests>()
            val resource = resourceManager.getResource(
                request.resourceArn,
                request.secretArn,
                request.transactionId,
                request.database
            )
            val executeStatementRequests = try {

                val statement = resource.connection.prepareStatement(request.sql, Statement.RETURN_GENERATED_KEYS)

                request.parameters?.forEachIndexed { index, sqlParameter ->
                    statement.setValue(index, sqlParameter.value, sqlParameter.typeHint)
                }

                statement.execute()
                val resultSet = statement.resultSet
                val updatedCount = if (statement.updateCount < 0) 0 else statement.updateCount
                val executeStatementRequests = if (resultSet is ResultSet) {
                    ExecuteStatementResponse(
                        updatedCount,
                        null,
                        statement.records,
                        if (request.includeResultMetadata) createColumnMetadata(resultSet) else null
                    )
                } else {
                    ExecuteStatementResponse(
                        updatedCount,
                        statement.generatedFields,
                    )
                }
                if (resource.transactionId == null) {
                    resource.commit()
                }
                executeStatementRequests
            } finally {
                if (resource.transactionId == null) {
                    resource.close()
                }
            }
            call.respond(executeStatementRequests)
        }
        post("/BatchExecute") {
            val request = call.receive<BatchExecuteStatementRequests>()
            val resource = resourceManager.getResource(
                request.resourceArn,
                request.secretArn,
                request.transactionId,
                request.database
            )
            try {
                // TODO: Execute SQL
                if (resource.transactionId == null) {
                    resource.commit()
                }
            } finally {
                if (resource.transactionId == null) {
                    resource.close()
                }
            }
            call.respond(BatchExecuteStatementResponse)
        }
    }
}

