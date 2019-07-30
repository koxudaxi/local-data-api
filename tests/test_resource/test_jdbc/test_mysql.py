from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Union
from unittest import TestCase, mock
from unittest.mock import Mock, call

import jaydebeapi
from local_data_api import convert_value
from local_data_api.exceptions import BadRequestException
from local_data_api.models import ColumnMetadata, ExecuteStatementResponse, Field
from local_data_api.resources.jdbc.mysql import MySQLJDBC

DATABASE_SETTINGS: Dict[str, Dict[str, Union[str, int]]] = {
    'SQLite': {'host': '', 'port': None, 'user_name': None, 'password': None}
}

if TYPE_CHECKING:
    pass


class TestResource(TestCase):
    def test_execute_insert(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = ''
        cursor_mock.rowcount = 1
        cursor_mock.fetchone.side_effect = [[0]]
        dummy = MySQLJDBC(connection_mock)
        self.assertEqual(
            dummy.execute("insert into users values (1, 'abc')"),
            ExecuteStatementResponse(numberOfRecordsUpdated=1, generatedFields=[]),
        )
        cursor_mock.execute.assert_has_calls(
            [
                call('SELECT LAST_INSERT_ID(NULL)'),
                call("insert into users values (1, 'abc')"),
                call('SELECT LAST_INSERT_ID()'),
            ]
        )
        cursor_mock.close.assert_called_once_with()

    def test_execute_insert_with_generated_field(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = ''
        cursor_mock.rowcount = 1
        cursor_mock.fetchone.side_effect = [[1]]
        dummy = MySQLJDBC(connection_mock)
        self.assertEqual(
            dummy.execute("insert into users (name) values ('abc')"),
            ExecuteStatementResponse(
                numberOfRecordsUpdated=1, generatedFields=[Field(longValue=1)]
            ),
        )
        cursor_mock.execute.assert_has_calls(
            [
                call('SELECT LAST_INSERT_ID(NULL)'),
                call("insert into users (name) values ('abc')"),
                call('SELECT LAST_INSERT_ID()'),
            ]
        )
        cursor_mock.close.assert_called_once_with()

    def test_execute_insert_with_params(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = ''
        cursor_mock.rowcount = 1
        cursor_mock.fetchone.side_effect = [[0]]
        dummy = MySQLJDBC(connection_mock)
        self.assertEqual(
            dummy.execute(
                "insert into users values (:id, :name)", {'id': 1, 'name': 'abc'}
            ),
            ExecuteStatementResponse(numberOfRecordsUpdated=1, generatedFields=[]),
        )
        cursor_mock.execute.assert_has_calls(
            [
                call('SELECT LAST_INSERT_ID(NULL)'),
                call("insert into users values (1, 'abc')"),
                call('SELECT LAST_INSERT_ID()'),
            ]
        )
        cursor_mock.close.assert_called_once_with()

    def test_execute_select(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = 1, 1, 1, 1, 1, 1, 1
        cursor_mock.fetchall.side_effect = [((1, 'abc'),)]
        dummy = MySQLJDBC(connection_mock, transaction_id='123')
        dummy.use_database = Mock()
        self.assertEqual(
            dummy.execute("select * from users", database_name='test'),
            ExecuteStatementResponse(
                numberOfRecordsUpdated=0,
                records=[[convert_value(1), convert_value('abc')]],
            ),
        )
        cursor_mock.execute.assert_has_calls(
            [call('SELECT LAST_INSERT_ID(NULL)'), call('select * from users')]
        )
        cursor_mock.close.assert_called_once_with()

    def test_execute_select_with_include_metadata(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        meta_mock = Mock()
        cursor_mock._meta = meta_mock
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = (1, 2, 3, 4, 5, 6, 7), (8, 9, 10, 11, 12, 13, 14)
        cursor_mock.fetchall.side_effect = [((1, 'abc'),)]
        dummy = MySQLJDBC(connection_mock, transaction_id='123')
        dummy.use_database = Mock()
        with mock.patch(
            'local_data_api.resources.jdbc.mysql.create_column_metadata_set'
        ) as create_column_metadata_set_mock:
            create_column_metadata_set_mock.side_effect = [
                [
                    ColumnMetadata(
                        arrayBaseColumnType=0,
                        isAutoIncrement=False,
                        isCaseSensitive=False,
                        isCurrency=False,
                        isSigned=False,
                        label=1,
                        name=1,
                        precision=5,
                        scale=6,
                        tableName=None,
                        type=None,
                        typeName=None,
                    ),
                    ColumnMetadata(
                        arrayBaseColumnType=0,
                        isAutoIncrement=False,
                        isCaseSensitive=False,
                        isCurrency=False,
                        isSigned=False,
                        label=8,
                        name=8,
                        precision=12,
                        scale=13,
                        tableName=None,
                        type=None,
                        typeName=None,
                    ),
                ]
            ]

            self.assertEqual(
                dummy.execute(
                    "select * from users",
                    database_name='test',
                    include_result_metadata=True,
                ),
                ExecuteStatementResponse(
                    numberOfRecordsUpdated=0,
                    records=[[convert_value(1), convert_value('abc')]],
                    columnMetadata=[
                        ColumnMetadata(
                            arrayBaseColumnType=0,
                            isAutoIncrement=False,
                            isCaseSensitive=False,
                            isCurrency=False,
                            isSigned=False,
                            label=1,
                            name=1,
                            precision=5,
                            scale=6,
                            tableName=None,
                            type=None,
                            typeName=None,
                        ),
                        ColumnMetadata(
                            arrayBaseColumnType=0,
                            isAutoIncrement=False,
                            isCaseSensitive=False,
                            isCurrency=False,
                            isSigned=False,
                            label=8,
                            name=8,
                            precision=12,
                            scale=13,
                            tableName=None,
                            type=None,
                            typeName=None,
                        ),
                    ],
                ),
            )

            create_column_metadata_set_mock.assert_called_once_with(meta_mock)
            cursor_mock.execute.assert_has_calls(
                [call('SELECT LAST_INSERT_ID(NULL)'), call('select * from users')]
            )
            cursor_mock.close.assert_called_once_with()

    def test_execute_exception_1(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        error = jaydebeapi.DatabaseError('error_message')
        error.args = ['error_message']
        cursor_mock.execute.side_effect = [0, error]
        connection_mock.cursor.side_effect = [cursor_mock]
        dummy = MySQLJDBC(connection_mock, transaction_id='123')
        with self.assertRaises(BadRequestException):
            try:
                dummy.execute("select * from users")
            except Exception as e:
                self.assertEqual(e.message, 'error_message')
                raise
        cursor_mock.execute.assert_has_calls(
            [call('SELECT LAST_INSERT_ID(NULL)'), call('select * from users')]
        )
        cursor_mock.close.assert_called_once_with()

    def test_execute_exception_2(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        error = jaydebeapi.DatabaseError('error')
        cause = Mock()
        cause.cause.message = 'cause_error_message'
        inner_error = Mock()
        inner_error.args = [cause]
        error.args = [inner_error]
        cursor_mock.execute.side_effect = [0, error]
        connection_mock.cursor.side_effect = [cursor_mock]
        dummy = MySQLJDBC(connection_mock, transaction_id='123')
        with self.assertRaises(BadRequestException):
            try:
                dummy.execute("select * from users")
            except Exception as e:
                self.assertEqual(e.message, 'cause_error_message')
                raise
        cursor_mock.execute.assert_has_calls(
            [call('SELECT LAST_INSERT_ID(NULL)'), call('select * from users')]
        )
        cursor_mock.close.assert_called_once_with()

    def test_execute_exception_3(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [jaydebeapi.DatabaseError()]
        dummy = MySQLJDBC(connection_mock, transaction_id='123')
        with self.assertRaises(BadRequestException):
            dummy.execute("select * from users")
        cursor_mock.close.assert_not_called()

    def test_execute_exception_4(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        error = jaydebeapi.DatabaseError('error')
        inner_error = Mock()
        inner_error.args = ['inner_error_message']
        error.args = [inner_error]
        cursor_mock.execute.side_effect = [0, error]
        connection_mock.cursor.side_effect = [cursor_mock]
        dummy = MySQLJDBC(connection_mock, transaction_id='123')
        with self.assertRaises(BadRequestException):
            try:
                dummy.execute("select * from users")
            except Exception as e:
                self.assertEqual(e.message, 'inner_error_message')
                raise
        cursor_mock.execute.assert_has_calls(
            [call('SELECT LAST_INSERT_ID(NULL)'), call('select * from users')]
        )
        cursor_mock.close.assert_called_once_with()
