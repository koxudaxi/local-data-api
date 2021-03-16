package com.koxudaxi.localDataApi

import java.util.*
import kotlin.test.*

class ModelsTest {
    @Test
    fun testSqlParameter() {
        assertEquals("aGVsbG8=", SqlParameter("value", Field(blobValue = "aGVsbG8=")).value.blobValue)
        assertEquals("DECIMAL", SqlParameter("value", Field(stringValue = "123"), typeHint = "DECIMAL").typeHint)
        assertEquals(null, SqlParameter("value", Field(stringValue = "123")).typeHint)
        assertEquals(null, SqlParameter("value", Field(stringValue = "123"), typeHint = null).typeHint)
        // hello as bytes
        assertEquals(
            byteArrayOf(104, 101, 108, 108, 111).toList(),
            (SqlParameter("value",Field(blobValue = "aGVsbG8=")).castValue as ByteArray).toList())
        assertEquals("dog", SqlParameter("value", Field(stringValue = "dog")).castValue)
        assertEquals((123).toDouble(), SqlParameter("value", Field(doubleValue = (123).toDouble())).castValue)
        assertEquals((123).toLong(), SqlParameter("value", Field(longValue = (123).toLong())).castValue)
        assertEquals(true, SqlParameter("value", Field(booleanValue = true)).castValue)
        assertEquals(java.sql.Date.valueOf("2020-10-12"),
            SqlParameter("value", Field(stringValue = "2020-10-12"), typeHint = "DATE").castValue)
        assertEquals((123).toBigDecimal(),
            SqlParameter("value", Field(stringValue = "123"), typeHint = "DECIMAL").castValue)
        assertEquals(java.sql.Time.valueOf("12:23:45"),
            SqlParameter("value", Field(stringValue = "12:23:45"), typeHint = "TIME").castValue)
        assertEquals(java.sql.Timestamp.valueOf("2021-03-10 12:23:45.968123"),
            SqlParameter("value", Field(stringValue = "2021-03-10 12:23:45.968123"), typeHint = "TIMESTAMP").castValue)
        assertEquals(UUID.fromString("abc08ed7-d834-47e8-9ef1-87b88bd774fb"),
            SqlParameter("value",
                Field(stringValue = "abc08ed7-d834-47e8-9ef1-87b88bd774fb"),
                typeHint = "UUID").castValue)
        assertEquals("dog", SqlParameter("value", Field(stringValue = "dog"), typeHint = "unknown").castValue)
        assertEquals(null, SqlParameter("value", Field(isNull = true)).castValue)
        assertEquals(true, SqlParameter("value", Field(isNull = true)).value.isNull)
    }
}

