package com.koxudaxi.localDataApi

import kotlin.test.*

class SecretManagerTest {
    @Test
    fun testGetSecretNotfound() {
        assertFailsWith(BadRequestException::class) {
            SecretManager().getSecret("not found")
        }
    }
}

