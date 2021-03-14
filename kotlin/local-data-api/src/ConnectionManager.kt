package com.koxudaxi.local_data_api

import java.sql.Connection
import java.sql.DriverManager
import java.util.*
import kotlin.random.Random

class ConnectionManager private constructor() {
    private val transactionIdCharacters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ/=+"
    private val transactionIdLength: Int = 184
    fun createTransactionId(): String = transactionIdCharacters.random(Random(transactionIdLength)).toString()

    fun createConnection(
        url: String, user: String, password: String?, database: String?
    ): Connection {
        return DriverManager.getConnection(database?.let { url + it } ?: url, user, password)
            .apply {
                this.autoCommit = false;
            }
    }

    fun setConnection(transactionId: String, connection: Connection) {
        connectionPool[transactionId] = connection
    }

    fun getConnection(transactionId: String): Connection {
        return connectionPool[transactionId] ?: throw BadRequestException("Invalid transaction ID")
    }

    fun deleteConnection(transactionId: String) {
        connectionPool.remove(transactionId)
    }

    fun hasConnection(transactionId: String?): Boolean {
        return transactionId is String && transactionId in connectionPool
    }

    private val connectionPool: MutableMap<String, Connection> = Collections.synchronizedMap(HashMap())

    companion object {
        val INSTANCE = ConnectionManager()
    }
}