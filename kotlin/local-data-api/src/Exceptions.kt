package com.koxudaxi.local_data_api

import io.ktor.http.*

abstract class DataAPIException(override val message: String?): Exception(){
    abstract val statusCode: HttpStatusCode
    val code: String = this::class.java.name
}

class BadRequestException(override val message: String?) : DataAPIException(message) {
    override val statusCode: HttpStatusCode = HttpStatusCode.BadRequest
}

class ForbiddenException(override val message: String?)  : DataAPIException(message) {
    override val statusCode: HttpStatusCode = HttpStatusCode.Forbidden
}

class InternalServerErrorException(override val message: String = "InternalServerError") : DataAPIException(message) {
    override val statusCode: HttpStatusCode = HttpStatusCode.InternalServerError
}


class NotFoundException(override val message: String?) : DataAPIException(message) {
    override val statusCode: HttpStatusCode = HttpStatusCode.NotFound
}

class ServiceUnavailableError(override val message: String?) : DataAPIException(message) {
    override val statusCode: HttpStatusCode = HttpStatusCode.ServiceUnavailable
}
