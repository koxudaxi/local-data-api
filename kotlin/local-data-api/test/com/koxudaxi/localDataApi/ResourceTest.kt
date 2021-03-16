package com.koxudaxi.localDataApi

import kotlin.test.*
import io.mockk.*
import org.junit.After
import org.junit.Before

class ResourceTest {
    @Test
    fun testResourceConfig() {
        val config = Resource.Config("mysql", "abc", "localhost", 3306)
        assertEquals("jdbc:mysql://localhost:3306/", config.url)
        assertEquals(config.jdbcName, "mysql")
        assertEquals(config.resourceArn, "abc")
        assertEquals(config.host, "localhost")
        assertEquals(config.port, 3306)
    }
}

