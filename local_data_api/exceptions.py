from abc import ABC
from typing import Optional, Dict

from fastapi import HTTPException


class DataAPIException(HTTPException, ABC):
    STATUS_CODE: int

    def __init__(self, message: str, headers: Optional[Dict] = None):
        self.message: str = message
        super().__init__(status_code=self.STATUS_CODE, headers=headers)

    @property
    def code(self) -> str:
        return self.__class__.__name__


class BadRequestException(DataAPIException):
    STATUS_CODE = 400


class ForbiddenException(DataAPIException):
    STATUS_CODE = 403


class InternalServerErrorException(DataAPIException):
    def __init__(self, message: Optional[str] = None):
        super().__init__(message or 'InternalServerError')

    STATUS_CODE = 500


class NotFoundException(DataAPIException):
    STATUS_CODE = 404


class ServiceUnavailableError(DataAPIException):
    STATUS_CODE = 503
