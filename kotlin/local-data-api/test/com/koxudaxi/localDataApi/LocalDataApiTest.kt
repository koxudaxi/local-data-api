package com.koxudaxi.localDataApi

import kotlin.test.*
import io.mockk.*
import io.mockk.mockk
import org.junit.After
import java.sql.Connection
import java.sql.ResultSet
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
        assertEquals(listOf(Types.TIMESTAMP), DATETIME)
        assertEquals(listOf(Types.TIMESTAMP_WITH_TIMEZONE), DATETIME_TZ)
    }

    @Test
    fun testConvertOffsetDatetimeToUTC() {
        assertEquals("2021-03-10 20:41:04.123456", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.123456+02"))
        assertEquals("2021-03-10 22:41:04.123456", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.123456+00"))
        assertEquals("2021-03-10 20:41:04.12345", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.123450+02"))
        assertEquals("2021-03-10 20:41:04.1234", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.123400+02"))
        assertEquals("2021-03-10 20:41:04.123", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.123000+02"))
        assertEquals("2021-03-10 20:41:04.12", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.120000+02"))
        assertEquals("2021-03-10 20:41:04.1", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.100000+02"))
        assertEquals("2021-03-10 20:41:04", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.000000+02"))
        assertEquals("2021-03-10 20:41:04.12345", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.12345+02"))
        assertEquals("2021-03-10 20:41:04.1234", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.1234+02"))
        assertEquals("2021-03-10 20:41:04.123", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.123+02"))
        assertEquals("2021-03-10 20:41:04.12", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.12+02"))
        assertEquals("2021-03-10 20:41:04.1", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.1+02"))
        assertEquals("2021-03-10 20:41:04", convertOffsetDatetimeToUTC("2021-03-10 22:41:04+02"))
        assertEquals("2021-03-10 20:41:04.00005", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.000050+02"))
        assertEquals("2021-03-10 20:41:04.0004", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.000400+02"))
        assertEquals("2021-03-10 20:41:04.003", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.003000+02"))
        assertEquals("2021-03-10 20:41:04.02", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.020000+02"))
        assertEquals("2021-03-10 20:41:04", convertOffsetDatetimeToUTC("2021-03-10 22:41:04.000000+02"))
    }

    @Test
    fun testCreateField() {
        // For PostgreSQL
        val mock = mockk<ResultSet>(relaxed = true)
        every {mock.metaData.getColumnType(1) } returns Types.TIMESTAMP
        every {mock.metaData.getColumnTypeName(1) } returns "timestamptz"
        every {mock.getString(1) } returns "2021-03-10 22:41:04.123456+02"
        assertEquals(createField(mock, 1).stringValue, "2021-03-10 20:41:04.123456")
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
            Resource(Resource.Config("mysql", "abc", "localhost", 1234, emptyMap()), "user", "pass", null, null, "xyz")
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
            Resource(Resource.Config("mysql",
                "arn:aws:rds:us-east-1:123456789012:cluster:dummy",
                "127.0.0.1",
                3306,
                emptyMap()),
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
            Resource(Resource.Config("postgresql", "abc", "localhost", 1234, mapOf("stringtype" to "unspecified")),
                "user",
                "pass",
                null,
                null,
                "xyz")
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
                5432, mapOf("stringtype" to "unspecified")), "postgres", "example", null, null, "xyz")
        } returns resource

        val env = mapOf(
            "ENGINE" to "POSTGRESQL",
        )
        every { System.getenv() } returns env
        setup()
    }
}

