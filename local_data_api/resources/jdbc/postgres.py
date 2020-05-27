from typing import Any

import jaydebeapi
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Dialect

from local_data_api.models import Field
from local_data_api.resources.jdbc import JDBC
from local_data_api.resources.resource import register_resource_type


@register_resource_type
class PostgreSQLJDBC(JDBC):
    DRIVER = 'org.postgresql.Driver'
    JDBC_NAME = 'jdbc:postgresql'
    DIALECT: Dialect = postgresql.dialect(paramstyle='named')

    @staticmethod
    def reset_generated_id(cursor: jaydebeapi.Cursor) -> None:
        pass

    @staticmethod
    def last_generated_id(cursor: jaydebeapi.Cursor) -> int:
        return 0

    def get_field_from_value(self, value: Any) -> Field:
        if type(value).__name__.endswith('UUID'):
            return Field(stringValue=str(value))
        elif type(value).__name__.endswith('PGobject'):
            return Field(stringValue=str(value))
        elif type(value).__name__.endswith('PgArray'):
            return Field(stringValue=str(value))
        else:
            return super().get_field_from_value(value)
