from typing import Any, Optional

import jaydebeapi
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Dialect

from local_data_api.models import Field
from local_data_api.resources.jdbc import JDBC
from local_data_api.resources.resource import register_resource_type


@register_resource_type
class MySQLJDBC(JDBC):
    DRIVER = 'org.mariadb.jdbc.Driver'
    JDBC_NAME = 'jdbc:mariadb'
    DIALECT: Dialect = mysql.dialect(paramstyle='named')

    @staticmethod
    def reset_generated_id(cursor: jaydebeapi.Cursor) -> None:
        cursor.execute('SELECT LAST_INSERT_ID(NULL)')

    @staticmethod
    def last_generated_id(cursor: jaydebeapi.Cursor) -> int:
        cursor.execute("SELECT LAST_INSERT_ID()")
        return int(str(cursor.fetchone()[0]))

    def get_filed_from_jdbc_type(self, value: Any, jdbc_type: Optional[int]) -> Field:
        if type(value).__name__.endswith('BigInteger'):
            return Field(longValue=int(str(value)))
        else:
            return super().get_filed_from_jdbc_type(value, jdbc_type)
