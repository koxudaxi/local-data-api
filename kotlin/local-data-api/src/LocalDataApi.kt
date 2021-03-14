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
    return when (resultSet.metaData.getColumnType(index)) {
        in LONG -> Field(longValue = resultSet.getLong(index))
        in DOUBLE -> Field(doubleValue = resultSet.getDouble(index))
        in BOOLEAN -> Field(booleanValue = resultSet.getBoolean(index))
        in BLOB -> Field(blobValue = resultSet.getString(index))
        in DATETIME -> Field(stringValue = resultSet.getString(index).let {
            Regex("^[^.]+\\.\\d{3}|^[^.]+").find(it)?.value
        })
        else -> resultSet.getString(index)
            ?.let { Field(stringValue = it) }
            ?: Field(isNull = true)

    }
}

fun PreparedStatement.setValue(index: Int, field: Field, typeHint: String?) {
    when {
        field.blobValue != null -> this.setString(index, field.blobValue)
        field.booleanValue != null -> this.setBoolean(index, field.booleanValue)
        field.doubleValue != null -> this.setDouble(index, field.doubleValue)
        field.longValue != null -> this.setLong(index, field.longValue)
        field.stringValue != null -> {
            when (typeHint) {
                "DATE" -> this.setDate(index, java.sql.Date.valueOf(field.stringValue))
                "DECIMAL" -> this.setBigDecimal(index,  field.stringValue.toBigDecimal())
                "TIME" -> this.setTime(index,  java.sql.Time.valueOf(field.stringValue))
                "TIMESTAMP" -> this.setTimestamp(index,  java.sql.Timestamp.valueOf(field.stringValue))
                "UUID" -> this.setObject(index, UUID.fromString(field.stringValue))
                //TODO: JSON

            else -> this.setString(index, field.stringValue)
            }
        }
        else -> this.setNull(index, Types.NULL)
    }
}

val Statement.generatedFields: List<Field>?
    get() {
        this.generatedKeys.let { resultSet ->
            val fields = mutableListOf<Field>()
            while (resultSet.next()) {
                fields.add(createField(resultSet, 1))
            }
            return fields.takeIf { it.isNotEmpty() }
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