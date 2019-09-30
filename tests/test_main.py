import pytest
from local_data_api.main import app
from local_data_api.resources import SQLite
from local_data_api.resources.resource import (
    CONNECTION_POOL,
    RESOURCE_METAS,
    ResourceMeta,
)
from starlette.testclient import TestClient

client = TestClient(app)


@pytest.fixture
def mocked_mysql(mocker):
    RESOURCE_METAS.clear()
    CONNECTION_POOL.clear()

    secret = mocker.Mock()
    secret.user_name = 'test'
    secret.password = 'pw'
    meta = ResourceMeta(SQLite, lambda: None, 'localhost', 3306, 'test', 'pw')

    mocker.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta})
    mocker.patch('local_data_api.resources.resource.get_secret', return_value=secret)
    return


@pytest.fixture
def mocked_connection(mocker):
    mocked_connection = mocker.Mock()
    mocker.patch(
        'local_data_api.resources.resource.create_connection',
        return_value=mocked_connection,
    )
    return mocked_connection


@pytest.fixture
def mocked_connection_pool(mocker):
    connection_pool = {}
    mocker.patch('local_data_api.resources.resource.CONNECTION_POOL', connection_pool)
    return connection_pool


@pytest.fixture
def mocked_cursor(mocked_connection, mocker):
    cursor_mock = mocker.Mock()
    mocked_connection.cursor.side_effect = [cursor_mock]
    return cursor_mock


def test_execute_sql(mocked_mysql):
    with pytest.raises(NotImplementedError):
        client.post(
            "/ExecuteSql",
            json={
                'awsSecretStoreArn': 1,
                'dbClusterOrInstanceArn': 2,
                'sqlStatements': 3,
            },
        )


def test_begin_statement(mocked_mysql):
    response = client.post(
        "/BeginTransaction", json={'resourceArn': 'abc', 'secretArn': '1'}
    )
    assert response.status_code == 200
    response_json = response.json()
    assert len(response_json) == 1
    assert 'transactionId' in response_json
    assert isinstance(response_json['transactionId'], str) is True


def test_commit_transaction(mocked_mysql, mocker, mocked_connection_pool):
    mocked_connection_pool['2'] = mocker.Mock()
    response = client.post(
        "/CommitTransaction",
        json={'resourceArn': 'abc', 'secretArn': '1', 'transactionId': '2'},
    )
    assert response.status_code == 200
    assert response.json() == {'transactionStatus': 'Transaction Committed'}


def test_rollback_transaction(mocked_mysql, mocker, mocked_connection_pool):
    mocked_connection_pool['2'] = mocker.Mock()
    response = client.post(
        "/RollbackTransaction",
        json={'resourceArn': 'abc', 'secretArn': '1', 'transactionId': '2'},
    )
    assert response.status_code == 200
    assert response.json() == {'transactionStatus': 'Rollback Complete'}


def test_data_api_exception_handler():
    response = client.post(
        "/BeginTransaction", json={'resourceArn': 'abc', 'secretArn': '1'}
    )
    assert response.status_code == 400
    assert response.json() == {
        'code': 'BadRequestException',
        'message': 'HttpEndPoint is not enabled for abc',
    }


def test_execute_statement(mocked_mysql, mocked_cursor):
    mocked_cursor.description = 1, 1, 1, 1, 1, 1, 1
    mocked_cursor.fetchall.side_effect = [((1, 'abc'),)]

    response = client.post(
        "/Execute",
        json={'resourceArn': 'abc', 'secretArn': '1', 'sql': 'select * from users'},
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {
        'numberOfRecordsUpdated': 0,
        'records': [[{'longValue': 1}, {'stringValue': 'abc'}]],
    }


def test_execute_statement_with_parameters(mocked_mysql, mocked_cursor):
    mocked_cursor.description = 1, 1, 1, 1, 1, 1, 1
    mocked_cursor.fetchall.side_effect = [((1, 'abc'),)]

    response = client.post(
        "/Execute",
        json={
            'resourceArn': 'abc',
            'secretArn': '1',
            'sql': 'select * from users where (:id)',
            'parameters': [{'name': 'id', 'value': {'longValue': 1}}],
        },
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {
        'numberOfRecordsUpdated': 0,
        'records': [[{'longValue': 1}, {'stringValue': 'abc'}]],
    }


def test_execute_statement_with_transaction(
    mocked_mysql, mocked_connection, mocked_connection_pool, mocked_cursor
):
    mocked_cursor.description = 1, 1, 1, 1, 1, 1, 1
    mocked_cursor.fetchall.side_effect = [((1, 'abc'),)]
    mocked_connection_pool['2'] = mocked_connection

    response = client.post(
        "/Execute",
        json={
            'resourceArn': 'abc',
            'secretArn': '1',
            'sql': 'select * from users',
            'transactionId': '2',
        },
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {
        'numberOfRecordsUpdated': 0,
        'records': [[{'longValue': 1}, {'stringValue': 'abc'}]],
    }


def test_batch_execute_statement(mocked_mysql, mocked_cursor):
    mocked_cursor.description = ''
    mocked_cursor.rowcount = 1
    mocked_cursor.lastrowid = 0

    response = client.post(
        "/BatchExecute",
        json={
            'resourceArn': 'abc',
            'secretArn': '1',
            'sql': "insert into users values (1, 'abc')",
        },
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {'updateResults': []}


def test_batch_execute_statement_with_parameters(mocked_mysql, mocked_cursor):
    mocked_cursor.description = ''
    mocked_cursor.rowcount = 1
    mocked_cursor.lastrowid = 0

    response = client.post(
        "/BatchExecute",
        json={
            'resourceArn': 'abc',
            'secretArn': '1',
            'sql': "insert into users values (:id, :name)",
            'parameterSets': [
                [
                    {'name': 'id', 'value': {'longValue': 1}},
                    {'name': 'name', 'value': {'stringValue': 'abc'}},
                ]
            ],
        },
    )
    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {'updateResults': []}


def test_batch_execute_statement_with_transaction(
    mocked_mysql, mocked_connection, mocked_connection_pool, mocked_cursor
):
    mocked_cursor.description = ''
    mocked_cursor.rowcount = 1
    mocked_cursor.lastrowid = 1

    mocked_connection_pool['2'] = mocked_connection

    response = client.post(
        "/BatchExecute",
        json={
            'resourceArn': 'abc',
            'secretArn': '1',
            'sql': "insert into users (name) values (:name)",
            'transactionId': '2',
            'parameterSets': [[{'name': 'name', 'value': {'stringValue': 'abc'}}]],
        },
    )

    assert response.status_code == 200
    response_json = response.json()
    assert response_json == {'updateResults': [{'generatedFields': [{'longValue': 1}]}]}
