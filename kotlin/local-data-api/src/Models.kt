package com.koxudaxi.localDataApi

import kotlinx.serialization.*
import java.util.*


@Serializable
data class Field(
    val blobValue: String? = null, //  # Type: Base64-encoded binary data object
    val booleanValue: Boolean? = null,
    val doubleValue: Double? = null,
    val isNull: Boolean? = null,
    val longValue: Long? = null,
    val stringValue: String? = null,
)

@Serializable
data class SqlParameter(
    val name: String,
    val value: Field,
    val typeHint: String? = null,
) {
    val castValue: Any?
        get() {
            return when {
                value.blobValue != null -> value.blobValue
                value.booleanValue != null -> value.booleanValue
                value.doubleValue != null -> value.doubleValue
                value.longValue != null -> value.longValue
                value.stringValue != null -> {
                    when (typeHint) {
                        "DATE" -> java.sql.Date.valueOf(value.stringValue)
                        "DECIMAL" -> value.stringValue.toBigDecimal()
                        "TIME" -> java.sql.Time.valueOf(value.stringValue)
                        "TIMESTAMP" -> java.sql.Timestamp.valueOf(value.stringValue)
                        "UUID" -> UUID.fromString(value.stringValue)
                        //TODO: JSON
                        else -> value.stringValue
                    }
                }
                else -> null
            }
        }
}

//@Serializable
//data class ExecuteSqlRequest(
//    val awsSecretStoreArn: String,
//    val dbClusterOrInstanceArn: String,
//    val sqlStatements: String,
//    val database: String? = null,
//    val schema: String? = null,
//)

@Serializable
data class ExecuteStatementRequest(
    val resourceArn: String,
    val secretArn: String,
    val sql: String,
    val database: String? = null,
    val continueAfterTimeout: Boolean? = null,
    val includeResultMetadata: Boolean = false,
    val parameters: List<SqlParameter>? = null,
    val schema: String? = null,
    val transactionId: String? = null,
)


@Serializable
data class ColumnMetadata(
    val arrayBaseColumnType: Int?,
    val isAutoIncrement: Boolean?,
    val isCaseSensitive: Boolean?,
    val isCurrency: Boolean?,
    val isSigned: Boolean?,
    val label: String?,
    val name: String?,
    val nullable: Int?,
    val precision: Int?,
    val scale: Int?,
    val schemaName: String?,
    val tableName: String?,
    val type: Int?,
    val typeName: String?,
)

@Serializable
data class ExecuteStatementResponse(
    val numberOfRecordsUpdated: Int,
    val generatedFields: List<Field>? = null,
    val records: List<List<Field>>? = null,
    val columnMetadata: List<ColumnMetadata>? = null,
)

@Serializable
data class BeginTransactionRequest(
    val resourceArn: String,
    val secretArn: String,
    val schema: String? = null,
    val database: String? = null,
)

@Serializable
data class BeginTransactionResponse(
    val transactionId: String,
)

@Serializable
data class CommitTransactionRequest(
    val resourceArn: String,
    val secretArn: String,
    val transactionId: String,
)

@Serializable
data class CommitTransactionResponse(
    val transactionStatus: String, //'Transaction Committed' or 'Rollback Complete'
)

@Serializable
data class RollbackTransactionRequest(
    val resourceArn: String,
    val secretArn: String,
    val transactionId: String,
)

@Serializable
data class RollbackTransactionResponse(
    val transactionStatus: String, //'Transaction Committed' or 'Rollback Complete'
)

@Serializable
data class BatchExecuteStatementRequest(
    val resourceArn: String,
    val secretArn: String,
    val sql: String,
    val database: String? = null,
    val continueAfterTimeout: Boolean? = null,
    val includeResultMetadata: Boolean? = null,
    val parameterSets: List<List<SqlParameter>>? = null,
    val schema: String? = null,
    val transactionId: String? = null,
)

@Serializable
data class UpdateResult(
    val generatedFields: List<Field>,
)

@Serializable
data class BatchExecuteStatementResponse(
    val updateResults: List<UpdateResult>,
)


@Serializable
data class ErrorResponse(
    val message: String?,
    val code: String,
)
