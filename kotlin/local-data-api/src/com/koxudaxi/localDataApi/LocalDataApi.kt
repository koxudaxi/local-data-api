package com.koxudaxi.localDataApi

import java.sql.*
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.*

val LONG = listOf(Types.INTEGER, Types.TINYINT, Types.SMALLINT, Types.BIGINT)
val DOUBLE = listOf(Types.FLOAT, Types.REAL, Types.DOUBLE)

//val STRING = listOf(Types.DECIMAL, Types.CLOB)
val BOOLEAN = listOf(Types.BOOLEAN, Types.BIT)
val BLOB = listOf(Types.BLOB, Types.BINARY, Types.LONGVARBINARY, Types.VARBINARY)
val DATETIME = listOf(Types.TIMESTAMP)
val DATETIME_TZ = listOf(Types.TIMESTAMP_WITH_TIMEZONE)

fun convertOffsetDatetimeToUTC(input: String): String {
    val splitFormatUtc = OffsetDateTime.parse(input,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]x")
    )
        .atZoneSameInstant(ZoneOffset.UTC)
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))
        .split(".")
    return splitFormatUtc[0] + ".${splitFormatUtc[1]}".dropLastWhile { char -> char == '0' || char == '.' }
}

fun createField(resultSet: ResultSet, index: Int): Field {
    if (resultSet.getObject(index) == null) {
        return Field(isNull = true)
    }
    val value = resultSet.metaData.getColumnType(index)
    return when {
        value in LONG -> Field(longValue = resultSet.getLong(index))
        value in DOUBLE -> Field(doubleValue = resultSet.getDouble(index))
        value in BOOLEAN -> Field(booleanValue = resultSet.getBoolean(index))
        value in BLOB -> Field(blobValue = Base64.getEncoder().encodeToString(resultSet.getBytes(index)))
        value in DATETIME_TZ || (value in DATETIME && resultSet.metaData.getColumnTypeName(index) == "timestamptz")
        -> Field(stringValue = convertOffsetDatetimeToUTC(resultSet.getString(index)))
        value in DATETIME -> Field(stringValue = Regex("^[^.]+\\.\\d{3}|^[^.]+").find(resultSet.getString(index))!!.value)
        else -> Field(stringValue = resultSet.getString(index))
    }
}

fun isPostgreSQL(connection: Connection): Boolean = "PostgreSQL" in connection.metaData.databaseProductName

fun getGeneratedKeys(connection: Connection): List<Field> {
    if (isPostgreSQL(connection)) return emptyList()
    val resultSet = connection.createStatement().executeQuery("SELECT LAST_INSERT_ID()")
    resultSet.next()
    return IntRange(1, resultSet.metaData.columnCount).mapNotNull { index ->
        resultSet.getInt(index)
    }.filter { it > 0 }.map { Field(longValue = it.toLong()) }.toList()
}

val Statement.updateResults: List<List<Field>>
    get() {
        return this.generatedKeys.let { resultSet ->
            val results = mutableListOf<List<Field>>()
            while (resultSet.next()) {
                IntRange(1, resultSet.metaData.columnCount).map { index ->
                    createField(resultSet, index)
                }.toList().run { results.add(this) }
            }
            results
        }
    }


val Statement.records: List<List<Field>>
    get() {
        val records = mutableListOf<List<Field>>()
        while (resultSet.next()) {
            this.resultSet.metaData.let { metaData ->
                records.add(
                    IntRange(1, metaData.columnCount).map { index ->
                        createField(resultSet, index)
                    }.toList()
                )
            }
        }
        return records.toList()
    }

fun createColumnMetadata(resultSet: ResultSet): List<ColumnMetadata> {
    return resultSet.metaData.let {
        IntRange(1, resultSet.metaData.columnCount).map { index ->
            ColumnMetadata(
                0,
                it.isAutoIncrement(index),
                it.isCaseSensitive(index),
                it.isCurrency(index),
                it.isSigned(index),
                it.getColumnLabel(index),
                it.getColumnName(index),
                it.isNullable(index),
                it.getPrecision(index),
                it.getScale(index),
                it.getSchemaName(index),
                it.getTableName(index),
                it.getColumnType(index),
                it.getColumnTypeName(index),
            )
        }.toList()
    }
}


fun setup() {
    val env = System.getenv()
    val engine = env["ENGINE"] ?: "MySQL"
    val resourceArn = env["RESOURCE_ARN"] ?: "arn:aws:rds:us-east-1:123456789012:cluster:dummy"
    val secretArn = env["SECRET_ARN"] ?: "arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy"

    if ("mysql" in engine.toLowerCase()) {
        ResourceManager.INSTANCE.setResource(
            "mysql",
            resourceArn,
            env["MYSQL_HOST"] ?: "127.0.0.1",
            env["MYSQL_PORT"]?.toInt() ?: 3306,
            emptyMap()
        )
        SecretManager.INSTANCE.setSecret(
            env["MYSQL_USER"] ?: "root",
            env["MYSQL_PASSWORD"] ?: "example",
            secretArn
        )
    } else {
        ResourceManager.INSTANCE.setResource(
            "postgresql",
            resourceArn,
            env["POSTGRES_HOST"] ?: "127.0.0.1",
            env["POSTGRES_PORT"]?.toInt() ?: 5432,
            mapOf("stringtype" to "unspecified")
        )
        SecretManager.INSTANCE.setSecret(
            env["POSTGRES_USER"] ?: "postgres",
            env["POSTGRES_PASSWORD"] ?: "example",
            secretArn
        )
    }
}