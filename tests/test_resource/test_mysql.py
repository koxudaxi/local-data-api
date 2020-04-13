from __future__ import annotations

import pytest

from local_data_api.models import ColumnMetadata, ExecuteStatementResponse, Field
from local_data_api.resources import MySQL
from local_data_api.resources.resource import CONNECTION_POOL, RESOURCE_METAS


@pytest.fixture
def clear():
    RESOURCE_METAS.clear()
    CONNECTION_POOL.clear()


def test_create_connection_maker(mocker):
    mock_connect = mocker.patch('local_data_api.resources.mysql.pymysql.connect')
    connection_maker = MySQL.create_connection_maker(
        host='127.0.0.1',
        port=3306,
        user_name='root',
        password='pass',
        engine_kwargs={'auto_commit': True},
    )
    connection_maker(database='test')
    mock_connect.assert_called_once_with(
        auto_commit=True,
        host='127.0.0.1',
        password='pass',
        port=3306,
        user='root',
        database='test',
    )

    mock_connect = mocker.patch('local_data_api.resources.mysql.pymysql.connect')
    connection_maker = MySQL.create_connection_maker()
    connection_maker()
    mock_connect.assert_called_once_with()


def test_execute_select_with_include_metadata(clear, mocker):

    connection_mock = mocker.Mock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.side_effect = [cursor_mock]
    cursor_mock.description = (1, 2, 3, 4, 5, 6, 7), (8, 9, 10, 11, 12, 13, 14)
    cursor_mock.fetchall.side_effect = [((1, 'abc'),)]
    field_1 = mocker.Mock()
    field_1.name = '1'
    field_1.org_name = '1'
    field_1.flags = 2
    field_1.get_column_length.return_value = 5
    field_1.scale = 6
    field_1.table_name = None
    field_2 = mocker.Mock()
    field_2.name = '8'
    field_2.org_name = '8'
    field_2.flags = 2
    field_2.get_column_length.return_value = 12
    field_2.scale = 13
    field_2.table_name = None
    cursor_mock._result.fields = [field_1, field_2]
    dummy = MySQL(connection_mock, transaction_id='123')
    assert (
        dummy.execute("select * from users", include_result_metadata=True).dict()
        == ExecuteStatementResponse(
            numberOfRecordsUpdated=0,
            records=[[Field.from_value(1), Field.from_value('abc')]],
            columnMetadata=[
                ColumnMetadata(
                    arrayBaseColumnType=0,
                    isAutoIncrement=False,
                    isCaseSensitive=False,
                    isCurrency=False,
                    isSigned=False,
                    label='1',
                    name='1',
                    nullable=1,
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
                    label='8',
                    name='8',
                    nullable=1,
                    precision=12,
                    scale=13,
                    tableName=None,
                    type=None,
                    typeName=None,
                ),
            ],
        ).dict()
    )

    cursor_mock.execute.assert_called_once_with('select * from users')
    cursor_mock.close.assert_called_once_with()
