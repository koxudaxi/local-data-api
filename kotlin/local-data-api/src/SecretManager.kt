package com.koxudaxi.local_data_api

import java.security.MessageDigest
import java.util.*

class SecretManager {
    private fun createSecretArn(
        regionName: String = "us-east-1", account: String = "123456789012"
    ): String {
        val hash = MessageDigest.getInstance("SHA-1").digest()
        return "arn:aws:secretsmanager:${regionName}:${account}:secret:local-data-api${hash}"
    }


    fun registerSecret(
        userName: String?,
        password: String?,
        secretArn: String?,
    ): String {
        return (secretArn ?: createSecretArn()).apply {
            secrets[this] = Secret(userName, password)

        }
    }

    companion object {

        fun getSecret(secretArn: String): Secret {
            return secrets[secretArn] ?: throw BadRequestException(
                "Error fetching secret {secret_arn} : Secrets Manager can’t find the specified secret." +
                        " (Service: AWSSecretsManager; Status Code: 400; Error Code: " +
                        "ResourceNotFoundException; Request ID:  00000000-1111-2222-3333-44444444444)"
            )
        }
        private val secrets: MutableMap<String, Secret> = Collections.synchronizedMap(HashMap())
    }

    data class Secret(
        val userName: String?,
        val password: String?,
    )
}

