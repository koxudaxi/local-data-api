package com.koxudaxi.local_data_api

import java.sql.Connection

class Resource(val connection: Connection, val transactionId: String?) {
    private val connectionManager = ConnectionManager.INSTANCE
    fun begin(): String {
        val transactionId = connectionManager.createTransactionId()
        connectionManager.setConnection(transactionId, connection)
        return transactionId
    }

    fun commit() {
        connection.commit()
    }

    fun rollback() {
        connection.rollback()
    }

    fun close() {
        connection.close()
        if (transactionId is String && connectionManager.hasConnection(transactionId)) {
            connectionManager.deleteConnection(transactionId)
        }
    }

    data class Config(
        val jdbcName: String,
        val resourceArn: String,
        val host: String?,
        val port: Int?,
    ) {
        val url get() = "jdbc:${jdbcName}://${host}:${port}/"
    }
}
