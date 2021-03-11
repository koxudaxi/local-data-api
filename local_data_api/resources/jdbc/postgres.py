from typing import Any, Optional, Tuple

from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Dialect

from local_data_api.models import Field
from local_data_api.resources.jdbc import JDBC, jaydebeapi
from local_data_api.resources.resource import register_resource_type

PG_TYPES: Tuple[str, ...] = (
    'UUID',
    'PgArray',
    'PGobject',
    'PGbox',
    'PGcircle',
    'PGline',
    'PGlseg',
    'PGpath',
    'PGpoint',
    'PGpolygon',
)


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

    def get_filed_from_jdbc_type(self, value: Any, jdbc_type: Optional[int]) -> Field:
        type_ = type(value)
        for pg_type in PG_TYPES:
            if type_.__name__.endswith(pg_type):
                return Field(stringValue=str(value))
        return super().get_filed_from_jdbc_type(value, jdbc_type)
