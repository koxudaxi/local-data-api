package com.koxudaxi.local_data_api


import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.sql.Types
import java.util.*

val LONG = listOf(Types.INTEGER, Types.TINYINT, Types.SMALLINT, Types.BIGINT)
val DOUBLE = listOf(Types.FLOAT, Types.REAL, Types.DOUBLE)
val STRING = listOf(Types.DECIMAL, Types.CLOB)
val BOOLEAN = listOf(Types.BOOLEAN, Types.BIT)
val BLOB = listOf(Types.BLOB, Types.BINARY, Types.LONGVARBINARY, Types.VARBINARY)
val DATETIME = listOf(Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE)

fun createField(resultSet: ResultSet, index: Int): Field {
    if (resultSet.getObject(index) == null) {
        return Field(isNull = true)
    }
    return when (resultSet.metaData.getColumnType(index)) {
        in LONG -> Field(longValue = resultSet.getLong(index))
        in DOUBLE -> Field(doubleValue = resultSet.getDouble(index))
        in BOOLEAN -> Field(booleanValue = resultSet.getBoolean(index))
        in BLOB -> Field(blobValue = resultSet.getString(index))
        in DATETIME -> Field(stringValue = Regex("^[^.]+\\.\\d{3}|^[^.]+").find(resultSet.getString(index))!!.value)
        else -> {
            Field(stringValue = resultSet.getString(index))
        }
    }
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


val Statement.records: List<List<Field>>?
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
        return records.takeIf { it.isNotEmpty() }?.toList()
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

    when {
        "mysql" in engine.toLowerCase() -> {
            ResourceManager.INSTANCE.setResource(
                "mysql",
                resourceArn,
                env["MYSQL_HOST"] ?: "127.0.0.1",
                env["MYSQL_PORT"]?.toInt() ?: 3306,
            )
            SecretManager.INSTANCE.setSecret(
                env["MYSQL_USER"] ?: "root",
                env["MYSQL_PASSWORD"] ?: "example",
                secretArn
            )
        }
        else -> {
            ResourceManager.INSTANCE.setResource(
                "postgresql",
                resourceArn,
                env["POSTGRES_HOST"] ?: "127.0.0.1",
                env["POSTGRES_PORT"]?.toInt() ?: 5432,
            )
            SecretManager.INSTANCE.setSecret(
                env["POSTGRES_USER"] ?: "postgres",
                env["POSTGRES_PASSWORD"] ?: "example",
                secretArn
            )
        }
    }
}