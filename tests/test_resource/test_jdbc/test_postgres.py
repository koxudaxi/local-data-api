from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Union

import jaydebeapi
import pytest

from local_data_api.exceptions import BadRequestException
from local_data_api.models import ColumnMetadata, ExecuteStatementResponse, Field
from local_data_api.resources.jdbc.postgres import PostgreSQLJDBC
from tests.test_resource.test_resource import helper_default_test_field

DATABASE_SETTINGS: Dict[str, Dict[str, Union[str, int]]] = {
    'SQLite': {'host': '', 'port': None, 'user_name': None, 'password': None}
}

if TYPE_CHECKING:
    pass


@pytest.fixture
def mocked_connection(mocker):
    connection_mock = mocker.Mock()
    return connection_mock


@pytest.fixture
def mocked_cursor(mocked_connection, mocker):
    cursor_mock = mocker.Mock()
    mocked_connection.cursor.side_effect = [cursor_mock]
    return cursor_mock


def test_execute_insert(mocked_connection, mocked_cursor, mocker):
    mocked_cursor.description = ''
    mocked_cursor.rowcount = 1
    mocked_cursor.fetchone.side_effect = [[0]]
    dummy = PostgreSQLJDBC(mocked_connection)
    assert dummy.execute(
        "insert into users values (1, 'abc')"
    ) == ExecuteStatementResponse(numberOfRecordsUpdated=1, generatedFields=[])
    mocked_cursor.execute.assert_has_calls(
        [mocker.call("insert into users values (1, 'abc')")]
    )
    mocked_cursor.close.assert_called_once_with()

    mocked_cursor = mocker.Mock()
    mocked_connection.cursor.side_effect = [mocked_cursor]
    mocked_cursor.description = ''
    mocked_cursor.rowcount = 1
    mocked_cursor.fetchone.side_effect = [[0]]
    assert dummy.execute(
        "insert into users values (1, 'abc')"
    ) == ExecuteStatementResponse(numberOfRecordsUpdated=1, generatedFields=[])
    mocked_cursor.execute.assert_has_calls(
        [mocker.call("insert into users values (1, 'abc')")]
    )
    mocked_cursor.close.assert_called_once_with()


def test_execute_insert_with_generated_field(mocked_connection, mocked_cursor, mocker):
    mocked_cursor.description = ''
    mocked_cursor.rowcount = 1
    mocked_cursor.fetchone.side_effect = [[1]]
    dummy = PostgreSQLJDBC(mocked_connection)
    assert dummy.execute(
        "insert into users (name) values ('abc')"
    ) == ExecuteStatementResponse(numberOfRecordsUpdated=1, generatedFields=[])
    mocked_cursor.execute.assert_has_calls(
        [mocker.call("insert into users (name) values ('abc')")]
    )
    mocked_cursor.close.assert_called_once_with()


def test_execute_insert_with_params(mocked_connection, mocked_cursor, mocker):
    mocked_cursor.description = ''
    mocked_cursor.rowcount = 1
    mocked_cursor.fetchone.side_effect = [[0]]
    dummy = PostgreSQLJDBC(mocked_connection)
    assert dummy.execute(
        "insert into users values (:id, :name)", {'id': 1, 'name': 'abc'}
    ) == ExecuteStatementResponse(numberOfRecordsUpdated=1, generatedFields=[])
    mocked_cursor.execute.assert_has_calls(
        [mocker.call("insert into users values (1, 'abc')")]
    )
    mocked_cursor.close.assert_called_once_with()


def test_execute_select(mocked_connection, mocked_cursor, mocker):
    mocked_cursor.description = 1, 1, 1, 1, 1, 1, 1
    mocked_cursor.fetchall.side_effect = [((1, 'abc'),)]
    dummy = PostgreSQLJDBC(mocked_connection, transaction_id='123')
    dummy.create_column_metadata_set = create_column_metadata_set_mock = mocker.Mock()
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
    assert dummy.execute("select * from users",) == ExecuteStatementResponse(
        numberOfRecordsUpdated=0,
        records=[[dummy.get_field_from_value(1), dummy.get_field_from_value('abc')]],
    )

    mocked_cursor.execute.assert_has_calls([mocker.call('select * from users')])
    mocked_cursor.close.assert_called_once_with()


def test_execute_select_with_include_metadata(mocked_connection, mocked_cursor, mocker):
    mocked_cursor.description = (1, 2, 3, 4, 5, 6, 7), (8, 9, 10, 11, 12, 13, 14)
    mocked_cursor.fetchall.side_effect = [((1, 'abc'),)]
    dummy = PostgreSQLJDBC(mocked_connection, transaction_id='123')
    dummy.create_column_metadata_set = create_column_metadata_set_mock = mocker.Mock()
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

    assert dummy.execute(
        "select * from users", include_result_metadata=True
    ) == ExecuteStatementResponse(
        numberOfRecordsUpdated=0,
        records=[[dummy.get_field_from_value(1), dummy.get_field_from_value('abc')]],
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
    )

    create_column_metadata_set_mock.assert_called_once_with(mocked_cursor)
    mocked_cursor.execute.assert_has_calls([mocker.call('select * from users')])
    mocked_cursor.close.assert_called_once_with()


def test_execute_exception_1(mocked_connection, mocked_cursor, mocker):
    error = jaydebeapi.DatabaseError('error_message')
    error.args = ['error_message']
    mocked_cursor.execute.side_effect = [error]
    mocked_connection.cursor.side_effect = [mocked_cursor]
    dummy = PostgreSQLJDBC(mocked_connection, transaction_id='123')
    with pytest.raises(BadRequestException) as e:
        dummy.execute("select * from users")
    assert e.value.message == 'error_message'
    mocked_cursor.close.assert_called_once_with()


def test_execute_exception_2(mocked_connection, mocked_cursor, mocker):
    error = jaydebeapi.DatabaseError('error')
    cause = mocker.Mock()
    cause.cause.message = 'cause_error_message'
    inner_error = mocker.Mock()
    inner_error.args = [cause]
    error.args = [inner_error]
    mocked_cursor.execute.side_effect = [error]
    mocked_connection.cursor.side_effect = [mocked_cursor]
    dummy = PostgreSQLJDBC(mocked_connection, transaction_id='123')
    with pytest.raises(BadRequestException) as e:
        dummy.execute("select * from users")
    assert e.value.message == 'cause_error_message'
    mocked_cursor.close.assert_called_once_with()


def test_execute_exception_3(mocked_connection, mocked_cursor, mocker):
    mocked_connection.cursor.side_effect = [jaydebeapi.DatabaseError()]
    dummy = PostgreSQLJDBC(mocked_connection, transaction_id='123')
    with pytest.raises(BadRequestException):
        dummy.execute("select * from users")
    mocked_cursor.close.assert_not_called()


def test_execute_exception_4(mocked_connection, mocked_cursor, mocker):
    error = jaydebeapi.DatabaseError('error')
    inner_error = mocker.Mock()
    inner_error.args = ['inner_error_message']
    error.args = [inner_error]
    mocked_cursor.execute.side_effect = [error]
    mocked_connection.cursor.side_effect = [mocked_cursor]
    dummy = PostgreSQLJDBC(mocked_connection, transaction_id='123')
    with pytest.raises(BadRequestException) as e:
        dummy.execute("select * from users")
    assert e.value.message == 'inner_error_message'
    mocked_cursor.close.assert_called_once_with()


def test_from_value(mocker) -> None:
    connection_mock = mocker.Mock()
    dummy = PostgreSQLJDBC(connection_mock)

    class JavaUUID:
        def __init__(self, val: str):
            self._val: str = val

        def __str__(self) -> str:
            return self._val

    uuid = 'e9e1df6b-c6d3-4a34-9227-c27056d596c6'
    assert dummy.get_filed_from_jdbc_type(JavaUUID(uuid), None) == Field(
        stringValue=uuid
    )

    class PGobject:
        def __init__(self, val: str):
            self._val: str = val

        def __str__(self) -> str:
            return self._val

    assert dummy.get_filed_from_jdbc_type(PGobject("{}"), None) == Field(
        stringValue="{}"
    )

    class PgArray:
        def __init__(self, val: str):
            self._val: str = val

        def __str__(self) -> str:
            return self._val

    assert dummy.get_filed_from_jdbc_type(PgArray("{ITEM1,ITEM2}"), None) == Field(
        stringValue="{ITEM1,ITEM2}"
    )

    helper_default_test_field(dummy)
