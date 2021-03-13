package com.koxudaxi.local_data_api

import kotlinx.serialization.*


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
    val typeHint: String?
)

@Serializable
data class ExecuteSqlRequest(
    val awsSecretStoreArn: String,
    val dbClusterOrInstanceArn: String,
    val sqlStatements: String,
    val database: String?,
    val schema: String?
)

@Serializable
data class ExecuteStatementRequests(
    val resourceArn: String,
    val secretArn: String,
    val sql: String,
    val database: String? = null,
    val continueAfterTimeout: Boolean? = null,
    val includeResultMetadata: Boolean = false,
    val parameters: List<SqlParameter>? = null,
    val schema: String? = null,
    val transactionId: String? = null
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
    val schema: String?,
    val tableName: String?,
    val type: Int?,
    val typeName: String?,
)

@Serializable
data class ExecuteStatementResponse(
    val numberOfRecordsUpdated: Int,
    val generatedFields: List<Field>?,
    val records: List<List<Field>>?,
    val columnMetadata: List<ColumnMetadata>?,
)

@Serializable
data class BeginTransactionRequest(
    val resourceArn: String,
    val secretArn: String,
    val schema: String?,
    val database: String?,
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
    val transactionStatus: String //'Transaction Committed' or 'Rollback Complete'
)

@Serializable
data class RollbackTransactionRequest(
    val resourceArn: String,
    val secretArn: String,
    val transactionId: String,
)

@Serializable
data class RollbackTransactionResponse(
    val transactionStatus: String //'Transaction Committed' or 'Rollback Complete'
)

@Serializable
data class BatchExecuteStatementRequests(
    val resourceArn: String,
    val secretArn: String,
    val sql: String,
    val database: String?,
    val continueAfterTimeout: Boolean?,
    val includeResultMetadata: Boolean?,
    val parameterSets: List<List<SqlParameter>>?,
    val schema: String?,
    val transactionId: String?,
)

@Serializable
data class UpdateResult(
    val generatedFields: List<Field>
)

@Serializable
data class BatchExecuteStatementResponse(
    val updateResults: List<UpdateResult>
)


@Serializable
data class ErrorResponse(
    val message: String?,
    val code: String
)
