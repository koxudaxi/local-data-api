package com.koxudaxi.localDataApi

import kotlin.test.*
import io.mockk.*
import io.mockk.mockk
import org.junit.After
import org.junit.Before
import java.sql.Connection
import java.sql.DriverManager

class ConnectionManagerTest {
    private val dummyResourceArn = "arn:aws:rds:us-east-1:123456789012:cluster:test"
    private val dummySecretArn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:test"
    private val username = "username"
    private val password = "password"

    @Before
    fun setUp() {
        val jdbcName = "h2:./test;MODE=MySQL"
        ResourceManager.INSTANCE.setResource(jdbcName, dummyResourceArn, null, null)
        SecretManager.INSTANCE.setSecret(username, password, dummySecretArn)
        ConnectionManager.INSTANCE.createConnection("jdbc:${jdbcName}", username, password, null, null)
            .createStatement()
            .execute("DROP ALL OBJECTS")
    }

    @After
    fun tearDown() {
        unmockkAll()
    }
    @Test
    fun testCreateConnection() {
        val jdbcName = "h2:./test;MODE=MySQL;INIT=CREATE SCHEMA IF NOT EXISTS TEST"
        val connectionManager =
            ConnectionManager.INSTANCE.createConnection("jdbc:${jdbcName}", username, password, null, "TEST")
        assertEquals("TEST", connectionManager.schema)
        assertEquals(false, connectionManager.autoCommit)
    }

    @Test
    fun testGetConnectionNotFound() {
        assertFailsWith(BadRequestException::class) {
            ConnectionManager.INSTANCE.getConnection("not found xyz")
        }
    }

    @Test
    fun testHasConnectionNotFound() {
        assertFalse {
            ConnectionManager.INSTANCE.hasConnection("not found")
        }
        assertFalse {
            ConnectionManager.INSTANCE.hasConnection(null)
        }
    }

    @Test
    fun testGetConnection() {
        val connectionManager = spyk<ConnectionManager>()
        val connection = mockk<Connection>(relaxed = true)
        mockkStatic(DriverManager::class)
        every { DriverManager.getConnection("jdbc:mysql://127.0.0.1/test", "username", "password") } returns connection
        connectionManager.createConnection("jdbc:mysql://127.0.0.1/", "username", "password", "test", "testSchema")
    }
}

