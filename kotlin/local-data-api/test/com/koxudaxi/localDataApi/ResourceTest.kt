package com.koxudaxi.localDataApi

import kotlin.test.*

class ResourceTest {
    @Test
    fun testResourceConfig() {
        val config = Resource.Config("mysql", "abc", "localhost", 3306, emptyMap())
        assertEquals("jdbc:mysql://localhost:3306/", config.url)
        assertEquals(config.jdbcName, "mysql")
        assertEquals(config.resourceArn, "abc")
        assertEquals(config.host, "localhost")
        assertEquals(config.port, 3306)
        assertEquals(config.jdbcOptions, emptyMap())
    }
}

