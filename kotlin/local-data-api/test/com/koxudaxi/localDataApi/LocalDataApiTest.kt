package com.koxudaxi.localDataApi

import kotlin.test.*
import io.mockk.*
import io.mockk.mockk
import org.junit.After
import java.sql.Connection
import java.sql.Types

class LocalDataApiTest {
    @After
    fun tearDown() {
        unmockkAll()
    }

    @Test
    fun testTypes() {
        assertEquals(listOf(Types.INTEGER, Types.TINYINT, Types.SMALLINT, Types.BIGINT), LONG)
        assertEquals(listOf(Types.FLOAT, Types.REAL, Types.DOUBLE), DOUBLE)
        assertEquals(listOf(Types.BOOLEAN, Types.BIT), BOOLEAN)
        assertEquals(listOf(Types.BLOB, Types.BINARY, Types.LONGVARBINARY, Types.VARBINARY), BLOB)
        assertEquals(listOf(Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE), DATETIME)
    }

    @Test
    fun testMySQL() {
        mockkStatic(System::class)
        mockkObject(ResourceManager)
        mockkObject(ConnectionManager)
        val connection = mockk<Connection>(relaxed = true)
        val connectionManager = spyk<ConnectionManager>()
        every { ConnectionManager.INSTANCE } returns connectionManager
        every { connectionManager.getConnection("xyz") } returns connection
        val resourceManager = spyk<ResourceManager>()
        every { ResourceManager.INSTANCE } returns resourceManager
        val resource = mockk<Resource>(relaxed = true)
        mockkStatic(Resource::class)
        every {
            Resource(Resource.Config("mysql", "abc", "localhost", 1234), "user", "pass", null, null, "xyz")
        } returns resource

        val env = mapOf(
            "ENGINE" to "MYSQL",
            "RESOURCE_ARN" to "abc",
            "SECRET_ARN" to "xyz",
            "MYSQL_HOST" to "localhost",
            "MYSQL_PORT" to "1234",
            "MYSQL_USER" to "user",
            "MYSQL_PASSWORD" to "pass",
        )
        every { System.getenv() } returns env
        setup()
    }

    @Test
    fun testMySQLDefault() {
        mockkStatic(System::class)
        mockkObject(ResourceManager)
        mockkObject(ConnectionManager)
        val connection = mockk<Connection>(relaxed = true)
        val connectionManager = spyk<ConnectionManager>()
        every { ConnectionManager.INSTANCE } returns connectionManager
        every { connectionManager.getConnection("xyz") } returns connection
        val resourceManager = spyk<ResourceManager>()
        every { ResourceManager.INSTANCE } returns resourceManager
        val resource = mockk<Resource>(relaxed = true)
        mockkStatic(Resource::class)
        every {
            Resource(Resource.Config("mysql", "arn:aws:rds:us-east-1:123456789012:cluster:dummy", "127.0.0.1", 3306),
                "root",
                "example",
                null,
                null,
                "xyz")
        } returns resource

        val env = mapOf(
            "ENGINE" to "MYSQL",
        )
        every { System.getenv() } returns env
        setup()
    }

    @Test
    fun testPostgresql() {
        mockkStatic(System::class)
        mockkObject(ResourceManager)
        mockkObject(ConnectionManager)
        val connection = mockk<Connection>(relaxed = true)
        val connectionManager = spyk<ConnectionManager>()
        every { ConnectionManager.INSTANCE } returns connectionManager
        every { connectionManager.getConnection("xyz") } returns connection
        val resourceManager = spyk<ResourceManager>()
        every { ResourceManager.INSTANCE } returns resourceManager
        val resource = mockk<Resource>(relaxed = true)
        mockkStatic(Resource::class)
        every {
            Resource(Resource.Config("postgresql", "abc", "localhost", 1234), "user", "pass", null, null, "xyz")
        } returns resource

        val env = mapOf(
            "ENGINE" to "POSTGRESQL",
            "RESOURCE_ARN" to "abc",
            "SECRET_ARN" to "xyz",
            "POSTGRES_HOST" to "localhost",
            "POSTGRES_PORT" to "1234",
            "POSTGRES_USER" to "user",
            "POSTGRES_PASSWORD" to "pass",
        )
        every { System.getenv() } returns env
        setup()
    }

    @Test
    fun testPostgresqlDefault() {
        mockkStatic(System::class)
        mockkObject(ResourceManager)
        mockkObject(ConnectionManager)
        val connection = mockk<Connection>(relaxed = true)
        val connectionManager = spyk<ConnectionManager>()
        every { ConnectionManager.INSTANCE } returns connectionManager
        every { connectionManager.getConnection("xyz") } returns connection
        val resourceManager = spyk<ResourceManager>()
        every { ResourceManager.INSTANCE } returns resourceManager
        val resource = mockk<Resource>(relaxed = true)
        mockkStatic(Resource::class)
        every {
            Resource(Resource.Config("postgresql",
                "arn:aws:rds:us-east-1:123456789012:cluster:dummy",
                "127.0.0.1",
                5432), "postgres", "example", null, null, "xyz")
        } returns resource

        val env = mapOf(
            "ENGINE" to "POSTGRESQL",
        )
        every { System.getenv() } returns env
        setup()
    }
}

