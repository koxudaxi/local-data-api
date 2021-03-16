package com.koxudaxi.localDataApi

import java.util.*

class SecretManager {
    private val secrets: MutableMap<String, Secret> = Collections.synchronizedMap(HashMap())

    fun setSecret(
        userName: String,
        password: String?,
        secretArn: String,
    ): String {
        return (secretArn).apply {
            secrets[this] = Secret(userName, password)

        }
    }

    fun getSecret(secretArn: String): Secret {
        return secrets[secretArn] ?: throw BadRequestException(
            "Error fetching secret {secret_arn} : Secrets Manager can’t find the specified secret." +
                    " (Service: AWSSecretsManager; Status Code: 400; Error Code: " +
                    "ResourceNotFoundException; Request ID:  00000000-1111-2222-3333-44444444444)"
        )
    }

    data class Secret(
        val userName: String,
        val password: String?,
    )

    companion object {
        val INSTANCE = SecretManager()
    }
}

