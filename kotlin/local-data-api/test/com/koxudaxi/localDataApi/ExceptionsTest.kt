package com.koxudaxi.localDataApi

import io.ktor.http.*
import kotlin.test.*
import io.mockk.*
import io.mockk.mockk
import org.junit.After
import org.junit.Before
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

class ExceptionsTest {

    @Test
    fun testBadRequestExceptionFromSQLException() {
        assertEquals("Database error code: 0. Message: null",
            BadRequestException.fromSQLException(SQLException()).message
        )
        assertEquals("Database error code: 0. Message: test message",
            BadRequestException.fromSQLException(SQLException("test message")).message)

    }

    @Test
    fun testInternalServerErrorException() {
        assertEquals(HttpStatusCode.InternalServerError, InternalServerErrorException().statusCode)
    }

}

