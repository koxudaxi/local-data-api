package com.koxudaxi.localDataApi

import kotlin.test.*
import io.mockk.*
import org.junit.After
import org.junit.Before

class ResourceManagerTest {
    private val dummyResourceArn = "arn:aws:rds:us-east-1:123456789012:cluster:test"
    private val dummySecretArn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:test"
    private val username = "username"
    private val password = "password"

    @Before
    fun setUp() {
        val jdbcName = "h2:./test;MODE=MySQL"
        ResourceManager.INSTANCE.setResource(jdbcName, dummyResourceArn, null, null)
        SecretManager.INSTANCE.setSecret(username, password, dummySecretArn)
    }

    @After
    fun tearDown() {
        unmockkAll()
    }
    @Test
    fun testGetResourceNotfound() {
        assertFailsWith(BadRequestException::class) {
            ResourceManager.INSTANCE.getResource("not found", "username", null)
        }
    }
    @Test
    fun testGetResourceNotfoundWithTransaction() {
        mockkObject(ConnectionManager)
        val connectionManager = spyk<ConnectionManager>()
        every { ConnectionManager.INSTANCE } returns connectionManager
        every { connectionManager.hasConnection("xyz") } returns true
        assertFailsWith(InternalServerErrorException::class) {
            ResourceManager().getResource("not found", "username", null, transactionId = "xyz")
        }
    }
}

