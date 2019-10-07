from typing import Any, Dict, List, Optional

import jaydebeapi
from local_data_api.exceptions import BadRequestException
from local_data_api.models import ExecuteStatementResponse, Field
from local_data_api.resources.jdbc import JDBC, create_column_metadata_set
from local_data_api.resources.resource import register_resource_type
from sqlalchemy import text


@register_resource_type
class MySQLJDBC(JDBC):
    DRIVER = 'org.mariadb.jdbc.Driver'
    JDBC_NAME = 'jdbc:mariadb'

    @staticmethod
    def reset_generated_id(cursor: jaydebeapi.Cursor) -> None:
        cursor.execute('SELECT LAST_INSERT_ID(NULL)')

    @staticmethod
    def last_generated_id(cursor: jaydebeapi.Cursor) -> int:
        cursor.execute("SELECT LAST_INSERT_ID()")
        return int(str(cursor.fetchone()[0]))

    def autocommit_off(self, cursor: jaydebeapi.Cursor) -> None:
        cursor.execute('SET AUTOCOMMIT=0')
        self.autocommit = False

    def execute(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        database_name: Optional[str] = None,
        include_result_metadata: bool = False,
    ) -> ExecuteStatementResponse:
        try:
            if database_name:
                self.use_database(database_name)

            cursor: Optional[jaydebeapi.Cursor] = None
            try:
                cursor = self.connection.cursor()

                if self.autocommit:
                    self.autocommit_off(cursor)

                self.reset_generated_id(cursor)
                if params:
                    cursor.execute(self.create_query(sql, params))
                else:
                    cursor.execute(str(text(sql)))
                if cursor.description:
                    response = ExecuteStatementResponse(
                        numberOfRecordsUpdated=0,
                        records=[
                            [Field.from_value(column) for column in row]
                            for row in cursor.fetchall()
                        ],
                    )
                    if include_result_metadata:
                        meta = getattr(cursor, '_meta')
                        response.columnMetadata = create_column_metadata_set(meta)
                    return response
                else:
                    rowcount: int = cursor.rowcount
                    last_generated_id: int = self.last_generated_id(cursor)
                    generated_fields: List[Field] = []
                    if last_generated_id > 0:
                        generated_fields.append(Field.from_value(last_generated_id))
                    return ExecuteStatementResponse(
                        numberOfRecordsUpdated=rowcount,
                        generatedFields=generated_fields,
                    )
            finally:
                if cursor:  # pragma: no cover
                    cursor.close()

        except jaydebeapi.DatabaseError as e:
            message: str = 'Unknown'
            if len(getattr(e, 'args', [])):
                message = e.args[0]
                if len(getattr(e.args[0], 'args', [])):
                    message = e.args[0].args[0]
                    if getattr(e.args[0].args[0], 'cause', None):
                        message = e.args[0].args[0].cause.message
            raise BadRequestException(str(message))
