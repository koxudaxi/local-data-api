package com.koxudaxi.local_data_api

import java.util.*

class ResourceManager {
    private val resourceConfigs: MutableMap<String, Resource.Config> = Collections.synchronizedMap(HashMap())
    private val connectionManager = ConnectionManager.INSTANCE

    fun setResource(
        jdbcName: String,
        resourceArn: String,
        host: String,
        port: Int?,
    ) {
        resourceConfigs[resourceArn] = Resource.Config(jdbcName, resourceArn, host, port)
    }

    fun getResource(
        resourceArn: String,
        secretArn: String,
        transactionId: String?,
        database: String?,
    ): Resource {
        val config = resourceConfigs[resourceArn]
            ?: if (connectionManager.hasConnection(transactionId)) {
                throw InternalServerErrorException()
            } else {
                throw BadRequestException("HttpEndPoint is not enabled for $resourceArn")
            }

        val secret = try {
            SecretManager.INSTANCE.getSecret(secretArn)
        } catch (e: BadRequestException) {
            if (connectionManager.hasConnection(transactionId)) {
                throw InternalServerErrorException()
            }
            throw e
        }

        val connection = if (transactionId == null) {

            connectionManager.createConnection(config.url, secret.userName, secret.password, database)
        } else {
            connectionManager.getConnection(transactionId).let {
                if (database?.toLowerCase() != it.catalog?.toLowerCase()) {
                    throw  BadRequestException("Database name is not the same as when transaction was created")
                }
                it
            }
        }
        return Resource(config, connection, transactionId)
    }

    companion object {
        val INSTANCE = ResourceManager()
    }


}