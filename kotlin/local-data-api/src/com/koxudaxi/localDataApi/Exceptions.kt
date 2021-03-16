package com.koxudaxi.localDataApi

import io.ktor.http.*
import java.sql.SQLException

abstract class DataAPIException(override val message: String) : Exception() {
    abstract val statusCode: HttpStatusCode
    val code: String = this::class.java.name.substringAfterLast('.')
}

class BadRequestException(override val message: String) : DataAPIException(message) {
    override val statusCode: HttpStatusCode = HttpStatusCode.BadRequest

    companion object {
        fun fromSQLException(sqlException: SQLException): BadRequestException =
            BadRequestException(
                "Database error code: ${sqlException.errorCode}." +
                        " Message: ${sqlException.message?.substringAfter(") ")}"
            )
    }
}

//class ForbiddenException(override val message: String?) : DataAPIException(message) {
//    override val statusCode: HttpStatusCode = HttpStatusCode.Forbidden
//}

class InternalServerErrorException : DataAPIException("InternalServerError") {
    override val statusCode: HttpStatusCode = HttpStatusCode.InternalServerError
}


class NotFoundException(override val message: String) : DataAPIException(message) {
    override val statusCode: HttpStatusCode = HttpStatusCode.NotFound
}

//class ServiceUnavailableError(override val message: String?) : DataAPIException(message) {
//    override val statusCode: HttpStatusCode = HttpStatusCode.ServiceUnavailable
//}
