package com.koxudaxi.localDataApi

class Resource(
    config: Config,
    userName: String,
    password: String?,
    database: String?,
    schema: String?,
    val transactionId: String?,
) {
    private val connectionManager = ConnectionManager.INSTANCE

    val connection = if (transactionId == null || transactionId.isBlank()) {
        connectionManager.createConnection(config.url, userName, password, database, schema, config.jdbcOptions)
    } else {
        connectionManager.getConnection(transactionId)
    }

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
        val jdbcOptions: Map<String, String>
    ) {
        val url get() = if (host is String) "jdbc:${jdbcName}://${host}:${port}/" else "jdbc:${jdbcName}"
    }
}
