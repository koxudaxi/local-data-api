package com.koxudaxi.local_data_api

import io.ktor.application.*
import io.ktor.features.*
import io.ktor.response.*
import io.ktor.request.*
import io.ktor.routing.*

fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

const val TRANSACTION_COMMITTED = "Transaction Committed"
const val ROLLBACK_COMPLETE = "Rollback Complete"

@Suppress("unused") // Referenced in application.conf
@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {
    install(ContentNegotiation)
    install(StatusPages) {
        exception<DataAPIException> { cause ->
            call.respond(cause.statusCode, ErrorResponse(cause.message, cause.code))
        }

    }
    routing {
        val resourceManager = ResourceManager()

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
            call.respond(ExecuteStatementResponse)
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
