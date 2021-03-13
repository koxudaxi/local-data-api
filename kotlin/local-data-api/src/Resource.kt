package com.koxudaxi.local_data_api

import java.sql.Connection

class Resource(private val config: Config, val connection: Connection, val transactionId: String?) {
    val url =  "${config.jdbcName}://${config.host}:${config.port}/"
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
        if (transactionId is String && connectionManager.hasConnection(transactionId)){
            connectionManager.deleteConnection(transactionId)
        }
    }

    data class Config(
        val jdbcName: String,
        val resourceArn: String,
        val engineName: String,
        val host: String?,
        val port: Int?,
        val userName: String?,
        val password: String?,
        val jdbcJARPath: String,
    )
}
