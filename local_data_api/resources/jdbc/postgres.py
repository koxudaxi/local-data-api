import jaydebeapi
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Dialect

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
