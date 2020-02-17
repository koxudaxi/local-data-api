from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pytest
from local_data_api.exceptions import BadRequestException, InternalServerErrorException
from local_data_api.models import ColumnMetadata, ExecuteStatementResponse, Field
from local_data_api.resources import SQLite
from local_data_api.resources.resource import (
    CONNECTION_POOL,
    RESOURCE_METAS,
    Resource,
    ResourceMeta,
    create_column_metadata,
    create_resource_arn,
    delete_connection,
    get_connection,
    get_resource,
    get_resource_class,
    register_resource,
    set_connection,
)
from sqlalchemy.dialects import mysql

DATABASE_SETTINGS: Dict[str, Dict[str, Union[str, int]]] = {
    'SQLite': {'host': '', 'port': None, 'user_name': None, 'password': None}
}

if TYPE_CHECKING:
    from local_data_api.resources.resource import ConnectionMaker


class DummyResource(Resource):
    DIALECT = mysql.dialect(paramstyle='named')

    @classmethod
    def create_connection_maker(
        cls,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user_name: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        engine_kwargs: Dict[str, Any] = None,
    ) -> ConnectionMaker:
        pass


@pytest.fixture
def clear():
    RESOURCE_METAS.clear()
    CONNECTION_POOL.clear()


@pytest.fixture
def secrets(mocker):
    secret = mocker.Mock()
    secret.user_name = 'test'
    secret.password = 'pw'
    mocked_secrets = mocker.patch(
        'local_data_api.resources.resource.get_secret', return_value=secret
    )
    return mocked_secrets


def test_register_resource(clear) -> None:
    resource_arn: str = 'dummy_resource_arn'
    register_resource(
        resource_arn,
        'MySQL',
        host='localhost',
        port=3306,
        user_name='test',
        password='pw',
    )

    resource_meta: ResourceMeta = RESOURCE_METAS[resource_arn]
    assert resource_meta.host == 'localhost'
    assert resource_meta.port == 3306
    assert resource_meta.user_name == 'test'
    assert resource_meta.password == 'pw'


def test_get_resource(secrets, mocker) -> None:
    resource_arn: str = 'dummy_resource_arn'

    connection_maker = SQLite.create_connection_maker(
        'localhost', 3306, 'test', 'pw', {}
    )

    RESOURCE_METAS[resource_arn] = ResourceMeta(
        SQLite, connection_maker, 'localhost', 3306, 'test', 'pw'
    )

    mock_get_connection = mocker.patch(
        'local_data_api.resources.resource.get_connection'
    )
    new_connection = mocker.Mock()
    mock_get_connection.return_value = new_connection
    resource = get_resource(resource_arn, 'dummy', 'transaction')
    assert resource.connection, new_connection

    with pytest.raises(BadRequestException):
        get_resource(resource_arn, 'dummy', 'transaction', database='test')


def test_get_resource_exception(clear, secrets, mocker) -> None:
    resource_arn: str = 'dummy_resource_arn'

    connection_maker = SQLite.create_connection_maker()

    RESOURCE_METAS[resource_arn] = ResourceMeta(
        SQLite, connection_maker, 'localhost', 3306, 'test', 'pw'
    )

    with pytest.raises(BadRequestException):
        get_resource('invalid', 'dummy')

    with pytest.raises(InternalServerErrorException):
        CONNECTION_POOL['dummy'] = connection_maker()
        get_resource('invalid', 'dummy', 'dummy')
    del CONNECTION_POOL['dummy']

    with pytest.raises(BadRequestException):
        secrets.side_effect = BadRequestException('error')
        get_resource(resource_arn, 'dummy')

    with pytest.raises(InternalServerErrorException):
        secrets.side_effect = BadRequestException('error')
        CONNECTION_POOL['dummy'] = connection_maker()
        get_resource(resource_arn, 'dummy', 'dummy')

    secrets.side_effect = None
    secret = mocker.Mock()
    secret.user_name = 'invalid'
    secret.password = 'pw'

    secrets.return_value = secret
    with pytest.raises(BadRequestException):
        get_resource(resource_arn, 'dummy')

    secret = mocker.Mock()
    secret.user_name = 'test'
    secret.password = 'invalid'

    secrets.return_value = secret
    with pytest.raises(BadRequestException):
        get_resource(resource_arn, 'dummy')


def test_get_resource_class_exception(clear) -> None:
    with pytest.raises(Exception):
        get_resource_class('invalid_engine')


def test_create_resource_arn(clear):
    assert (
        create_resource_arn('us-east-1', '123456789012')[:57]
        == 'arn:aws:rds:us-east-1:123456789012:cluster:local-data-api'
    )


def test_get_database_invalid_resource_arn(clear) -> None:
    resource_arn: str = 'dummy_resource_arn'

    connection_maker = SQLite.create_connection_maker(
        'localhost', 3306, 'test', 'pw', {}
    )

    RESOURCE_METAS[resource_arn] = ResourceMeta(
        SQLite, connection_maker, 'localhost', 3306, 'test', 'pw'
    )

    with pytest.raises(BadRequestException):
        get_resource('invalid_arn', 'secret_arn')


def test_create_column_metadata(clear):
    assert create_column_metadata('name', 1, 2, 3, 4, 5, True) == ColumnMetadata(
        arrayBaseColumnType=0,
        isAutoIncrement=False,
        isCaseSensitive=False,
        isCurrency=False,
        isSigned=False,
        label='name',
        name='name',
        nullable=1,
        precision=4,
        scale=5,
        tableName=None,
        type=None,
        typeName=None,
        schema_=None,
    )


def test_set_connection(clear, mocker) -> None:
    connection = mocker.Mock()
    set_connection('abc', connection)
    assert CONNECTION_POOL['abc'] == connection


def test_get_connection(clear, mocker) -> None:
    connection = mocker.Mock()
    CONNECTION_POOL['abc'] = connection
    assert get_connection('abc') == connection


def test_get_connection_notfound(clear) -> None:
    with pytest.raises(BadRequestException):
        get_connection('abc')


def test_delete_connection(clear, mocker) -> None:
    connection = mocker.Mock()
    CONNECTION_POOL['abc'] = connection

    delete_connection('abc')
    assert 'abc' not in CONNECTION_POOL


def test_create_query(clear):
    query = DummyResource.create_query(
        'insert into users values (:id, :name)', {'id': 1, 'name': 'abc'}
    )
    assert query == "insert into users values (1, 'abc')"


def test_create_query_invalid_param(clear):
    with pytest.raises(BadRequestException) as cm:
        DummyResource.create_query('insert into users values (:id, :name)', {'id': 1})
    assert cm.value.message == 'Cannot find parameter: name'


def test_create_query_undefined_param(clear):
    query = DummyResource.create_query(
        'insert into users values (:id, :name)',
        {'id': 1, 'name': 'abc', 'undefined': 'abc'},
    )
    assert query == "insert into users values (1, 'abc')"


def test_transaction_id(clear, mocker):
    connection_mock = mocker.Mock()
    dummy = DummyResource(connection_mock, transaction_id='123')
    assert dummy.transaction_id == '123'


def test_connection(clear, mocker):
    connection_mock = mocker.Mock()
    dummy = DummyResource(connection_mock)
    assert dummy.connection == connection_mock


def test_create_transaction_id(clear):
    transaction_id: str = DummyResource.create_transaction_id()
    assert re.match(
        r'[abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ/=+]{184}$',
        transaction_id,
    )


def test_commit(clear, mocker):
    connection_mock = mocker.Mock()
    dummy = DummyResource(connection_mock)
    dummy.commit()
    assert connection_mock.commit.call_count == 1


def test_rollback(clear, mocker):
    connection_mock = mocker.Mock()
    dummy = DummyResource(connection_mock)
    dummy.rollback()
    assert connection_mock.rollback.call_count == 1


def test_begin(clear, mocker):
    create_transaction_id_mock = mocker.Mock(side_effect=['abc'])
    connection_mock = mocker.Mock()
    dummy = DummyResource(connection_mock)
    dummy.create_transaction_id = create_transaction_id_mock
    set_connection_mock = mocker.patch(
        'local_data_api.resources.resource.set_connection'
    )
    result = dummy.begin()
    assert result == 'abc'
    set_connection_mock.assert_called_once_with('abc', connection_mock)


def test_close(clear, mocker):
    connection_mock = mocker.Mock()
    dummy = DummyResource(connection_mock, 'abc')
    delete_connection_mock = mocker.patch(
        'local_data_api.resources.resource.delete_connection'
    )
    mocker.patch('local_data_api.resources.resource.CONNECTION_POOL', {'abc': ''})
    dummy.close()
    connection_mock.close.assert_called_once_with()
    delete_connection_mock.assert_called_once_with('abc')


def test_close_with_empty_connection_pool(clear, mocker):
    connection_mock = mocker.Mock()
    dummy = DummyResource(connection_mock, 'abc')
    mocker.patch('local_data_api.resources.resource.CONNECTION_POOL', {})
    dummy.close()
    connection_mock.close.assert_called_once_with()


def test_execute_insert(clear, mocker):
    connection_mock = mocker.Mock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.side_effect = [cursor_mock]
    cursor_mock.description = ''
    cursor_mock.rowcount = 1
    cursor_mock.lastrowid = 0
    dummy = DummyResource(connection_mock)
    assert dummy.execute(
        "insert into users values (1, 'abc')"
    ) == ExecuteStatementResponse(numberOfRecordsUpdated=1, generatedFields=[])
    cursor_mock.execute.assert_called_once_with("insert into users values (1, 'abc')")
    cursor_mock.close.assert_called_once_with()


def test_execute_insert_with_generated(clear, mocker):
    connection_mock = mocker.Mock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.side_effect = [cursor_mock]
    cursor_mock.description = ''
    cursor_mock.rowcount = 1
    cursor_mock.lastrowid = 1
    dummy = DummyResource(connection_mock)
    assert dummy.execute(
        "insert into users ('name') values ('abc')"
    ) == ExecuteStatementResponse(
        numberOfRecordsUpdated=1, generatedFields=[Field(longValue=1)]
    )
    cursor_mock.execute.assert_called_once_with(
        "insert into users ('name') values ('abc')"
    )
    cursor_mock.close.assert_called_once_with()


def test_execute_insert_with_params(clear, mocker):
    connection_mock = mocker.Mock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.side_effect = [cursor_mock]
    cursor_mock.description = ''
    cursor_mock.rowcount = 1
    cursor_mock.lastrowid = 1
    dummy = DummyResource(connection_mock)
    assert dummy.execute(
        "insert into users values (:id, :name)", {'id': 1, 'name': 'abc'}
    ) == ExecuteStatementResponse(
        numberOfRecordsUpdated=1, generatedFields=[Field(longValue=1)]
    )

    cursor_mock.execute.assert_called_once_with("insert into users values (1, 'abc')")
    cursor_mock.close.assert_called_once_with()


def test_execute_select(clear, mocker):
    connection_mock = mocker.Mock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.side_effect = [cursor_mock]
    cursor_mock.description = 1, 1, 1, 1, 1, 1, 1
    cursor_mock.fetchall.side_effect = [((1, 'abc'),)]
    dummy = DummyResource(connection_mock, transaction_id='123')
    assert dummy.execute("select * from users",) == ExecuteStatementResponse(
        numberOfRecordsUpdated=0,
        records=[[Field.from_value(1), Field.from_value('abc')]],
    )
    cursor_mock.execute.assert_called_once_with('select * from users')
    cursor_mock.close.assert_called_once_with()


def test_execute_select_with_include_metadata(clear, mocker):
    connection_mock = mocker.Mock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.side_effect = [cursor_mock]
    cursor_mock.description = (1, 2, 3, 4, 5, 6, 7), (8, 9, 10, 11, 12, 13, 14)
    cursor_mock.fetchall.side_effect = [((1, 'abc'),)]
    dummy = DummyResource(connection_mock, transaction_id='123')
    assert dummy.execute(
        "select * from users", include_result_metadata=True
    ).dict() == ExecuteStatementResponse(
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
    )

    cursor_mock.execute.assert_called_once_with('select * from users')
    cursor_mock.close.assert_called_once_with()


def test_execute_exception_1(clear, mocker):
    connection_mock = mocker.Mock()
    cursor_mock = mocker.Mock()
    error = Exception('error')
    error.orig = ['error_message']
    cursor_mock.execute.side_effect = error
    connection_mock.cursor.side_effect = [cursor_mock]
    dummy = DummyResource(connection_mock, transaction_id='123')
    with pytest.raises(BadRequestException):
        dummy.execute("select * from users")
    cursor_mock.execute.assert_called_once_with('select * from users')
    cursor_mock.close.assert_called_once_with()


def test_execute_exception_2(clear, mocker):
    connection_mock = mocker.Mock()
    cursor_mock = mocker.Mock()
    error = Exception('error')
    error_orig = mocker.Mock()
    error_orig.args = ['', 'error_message']
    error.orig = error_orig
    cursor_mock.execute.side_effect = error
    connection_mock.cursor.side_effect = [cursor_mock]
    dummy = DummyResource(connection_mock, transaction_id='123')
    with pytest.raises(BadRequestException) as e:
        dummy.execute("select * from users")
    assert e.value.message == 'error_message'
    cursor_mock.execute.assert_called_once_with('select * from users')
    cursor_mock.close.assert_called_once_with()


def test_execute_exception_3(clear, mocker):
    connection_mock = mocker.Mock()
    cursor_mock = mocker.Mock()
    error = Exception('error')
    inner_error = Exception('inner_error')
    error.args = [inner_error]
    cursor_mock.execute.side_effect = error
    connection_mock.cursor.side_effect = [cursor_mock]
    dummy = DummyResource(connection_mock, transaction_id='123')
    with pytest.raises(BadRequestException) as e:
        dummy.execute("select * from users")
    assert e.value.message == 'inner_error'

    cursor_mock.execute.assert_called_once_with('select * from users')
    cursor_mock.close.assert_called_once_with()


def test_execute_exception_4(clear, mocker):
    connection_mock = mocker.Mock()
    cursor_mock = mocker.Mock()
    error = Exception('')
    cursor_mock.execute.side_effect = error
    connection_mock.cursor.side_effect = [cursor_mock]
    dummy = DummyResource(connection_mock, transaction_id='123')
    with pytest.raises(BadRequestException) as e:
        dummy.execute("select * from users")
    assert e.value.message == 'Unknown'

    cursor_mock.execute.assert_called_once_with('select * from users')
    cursor_mock.close.assert_called_once_with()
