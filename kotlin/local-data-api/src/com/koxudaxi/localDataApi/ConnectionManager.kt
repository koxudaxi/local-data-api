package com.koxudaxi.localDataApi

import java.sql.Connection
import java.sql.DriverManager
import java.util.*
import java.util.Properties


class ConnectionManager {
    private val transactionIdCharacters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ/=+"
    private val transactionIdLength: Int = 184
    fun createTransactionId(): String =
        IntRange(1, transactionIdLength).map { transactionIdCharacters.random() }.joinToString("")

    fun createConnection(
        url: String,
        user: String,
        password: String?,
        database: String?,
        schema: String?,
        jdbcOptions: Map<String, String>,
    ): Connection {
        val props = Properties()
        props.setProperty("user", user)
        if (password is String) {
            props.setProperty("password", password)
        }
        jdbcOptions.entries.forEach {
            props.setProperty(it.key, it.value)
        }
        return DriverManager.getConnection(url + (database ?: ""), props)
            .apply {
                this.autoCommit = false
                if (schema is String) {
                    this.schema = schema
                }
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