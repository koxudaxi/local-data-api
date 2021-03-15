package com.koxudaxi.localDataApi

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
import org.httprpc.sql.Parameters

fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

const val TRANSACTION_COMMITTED = "Transaction Committed"
const val ROLLBACK_COMPLETE = "Rollback Complete"

val resourceManager = ResourceManager.INSTANCE
val secretManager = SecretManager.INSTANCE

@Suppress("unused") // Referenced in application.conf
@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {
    setup()
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

        post("/ExecuteSql") {
            throw NotFoundException("NotImplemented")
        }
        post("/BeginTransaction") {
            val request = call.receive<BeginTransactionRequest>()
            val secret = secretManager.getSecret(request.secretArn)
            val resource = resourceManager.getResource(request.resourceArn,
                secret.userName,
                secret.password,
                null,
                request.database,
                request.schema)
            val transactionId = resource.begin()
            call.respond(BeginTransactionResponse(transactionId))
        }
        post("/CommitTransaction") {
            val request = call.receive<CommitTransactionRequest>()
            val secret = secretManager.getSecret(request.secretArn)
            val resource =
                resourceManager.getResource(
                    request.resourceArn,
                    secret.userName,
                    secret.password,
                    request.transactionId
                )
            resource.commit()
            resource.close()
            call.respond(CommitTransactionResponse(TRANSACTION_COMMITTED))
        }
        post("/RollbackTransaction") {
            val request = call.receive<RollbackTransactionRequest>()
            val secret = secretManager.getSecret(request.secretArn)
            val resource =
                resourceManager.getResource(request.resourceArn,
                    secret.userName,
                    secret.password,
                    request.transactionId,
                    null)
            resource.rollback()
            resource.close()
            call.respond(RollbackTransactionResponse(ROLLBACK_COMPLETE))
        }
        post("/Execute") {
            val request = call.receive<ExecuteStatementRequest>()
            val secret = secretManager.getSecret(request.secretArn)
            val resource = resourceManager.getResource(
                request.resourceArn,
                secret.userName,
                secret.password,
                request.transactionId,
                request.database
            )
            val executeStatementResponse = try {
                val statement = if (request.parameters == null) {
                    resource.connection.prepareStatement(request.sql, Statement.RETURN_GENERATED_KEYS)
                } else {
                    val parameters = Parameters.parse(request.sql)
                    val statement =
                        resource.connection.prepareStatement(parameters.sql, Statement.RETURN_GENERATED_KEYS)
                    parameters.apply(statement, request.parameters.map { Pair(it.name, it.castValue) }.toMap())
                    statement
                }


                statement.execute()
                val resultSet = statement.resultSet
                val updatedCount = if (statement.updateCount < 0) 0 else statement.updateCount
                val executeStatementResponse = if (resultSet is ResultSet) {
                    ExecuteStatementResponse(
                        updatedCount,
                        null,
                        statement.records,
                        if (request.includeResultMetadata) createColumnMetadata(resultSet) else null
                    )
                } else {
                    ExecuteStatementResponse(
                        updatedCount,
                        statement.updateResults.lastOrNull() ?: emptyList(),
                    )
                }
                if (resource.transactionId == null) {
                    resource.commit()
                }
                executeStatementResponse
            } finally {
                if (resource.transactionId == null) {
                    resource.close()
                }
            }
            call.respond(executeStatementResponse)
        }
        post("/BatchExecute") {
            val request = call.receive<BatchExecuteStatementRequest>()
            val secret = secretManager.getSecret(request.secretArn)
            val resource = resourceManager.getResource(
                request.resourceArn,
                secret.userName,
                secret.password,
                request.transactionId,
                request.database
            )
            if (request.parameterSets == null) {
                if (resource.transactionId == null) {
                    resource.close()
                }
                call.respond(BatchExecuteStatementResponse(listOf()))
                return@post
            }
            val batchExecuteStatementResponse = try {
                val parameters = Parameters.parse(request.sql)
                val statement =
                    resource.connection.prepareStatement(parameters.sql, Statement.RETURN_GENERATED_KEYS)

                request.parameterSets.forEach { parameterSet ->
                    parameters.apply(statement, parameterSet.map { Pair(it.name, it.castValue) }.toMap())
                    statement.addBatch()
                    statement.clearParameters()
                }

                statement.executeBatch()

                val batchExecuteStatementResponse = BatchExecuteStatementResponse(
                    statement.updateResults.map { UpdateResult(it) }.toList()
                )
                if (resource.transactionId == null) {
                    resource.commit()
                }
                batchExecuteStatementResponse
            } finally {
                if (resource.transactionId == null) {
                    resource.close()
                }
            }
            call.respond(batchExecuteStatementResponse)
        }
    }
}