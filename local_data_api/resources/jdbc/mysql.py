import jaydebeapi
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Dialect

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
