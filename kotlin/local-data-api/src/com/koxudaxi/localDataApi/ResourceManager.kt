package com.koxudaxi.localDataApi

import java.util.*

class ResourceManager {
    private val resourceConfigs: MutableMap<String, Resource.Config> = Collections.synchronizedMap(HashMap())
    private val connectionManager = ConnectionManager.INSTANCE

    fun setResource(
        jdbcName: String,
        resourceArn: String,
        host: String?,
        port: Int?,
    ) {
        resourceConfigs[resourceArn] = Resource.Config(jdbcName, resourceArn, host, port)
    }

    fun getResource(
        resourceArn: String,
        userName: String,
        password: String?,
        transactionId: String? = null,
        database: String? = null,
        schema: String? = null,
    ): Resource {
        val config = resourceConfigs[resourceArn]
            ?: if (connectionManager.hasConnection(transactionId)) {
                throw InternalServerErrorException()
            } else {
                throw BadRequestException("HttpEndPoint is not enabled for $resourceArn")
            }

        return Resource(config, userName, password, database, schema, transactionId)
    }

    companion object {
        val INSTANCE = ResourceManager()
    }
}