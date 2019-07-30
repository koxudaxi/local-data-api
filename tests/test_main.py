from unittest import TestCase, mock
from unittest.mock import Mock

from starlette.testclient import TestClient

from local_data_api.main import app
from local_data_api.resources import MySQL, SQLite
from local_data_api.resources.resource import CONNECTION_POOL, RESOURCE_METAS, ResourceMeta

client = TestClient(app)


class TestMain(TestCase):
    def setUp(self) -> None:
        RESOURCE_METAS.clear()
        CONNECTION_POOL.clear()

    def test_execute_sql(self):
        with self.assertRaises(NotImplementedError):
            client.post("/ExecuteSql", json={'awsSecretStoreArn': 1, 'dbClusterOrInstanceArn': 2,
                                             'sqlStatements': 3})

    def test_begin_statement(self):
        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'
        meta = ResourceMeta(SQLite, lambda: None, 'localhost', 3306, 'test', 'pw')

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret:
            mock_get_secret.return_value = secret
            response = client.post("/BeginTransaction", json={'resourceArn': 'abc', 'secretArn': '1'})
            self.assertEqual(response.status_code, 200)
            response_json = response.json()
            self.assertEqual(len(response_json), 1)
            self.assertTrue('transactionId' in response_json)
            self.assertTrue(isinstance(response_json['transactionId'], str))

    def test_commit_transaction(self):
        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'
        meta = ResourceMeta(SQLite, lambda: None, 'localhost', 3306, 'test', 'pw')
        connection_mock = Mock()

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret, \
                mock.patch('local_data_api.resources.resource.CONNECTION_POOL', {'2': connection_mock}):
            mock_get_secret.return_value = secret
            response = client.post("/CommitTransaction", json={'resourceArn': 'abc', 'secretArn': '1',
                                                               'transactionId': '2'})
            self.assertEqual(response.status_code, 200)
            self.assertDictEqual(response.json(), {'transactionStatus': 'Transaction Committed'})

    def test_rollback_transaction(self):
        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'
        meta = ResourceMeta(SQLite, lambda: None, 'localhost', 3306, 'test', 'pw')
        connection_mock = Mock()

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret, \
                mock.patch('local_data_api.resources.resource.CONNECTION_POOL', {'2': connection_mock}):
            mock_get_secret.return_value = secret
            response = client.post("/RollbackTransaction", json={'resourceArn': 'abc', 'secretArn': '1',
                                                                 'transactionId': '2'})
            self.assertEqual(response.status_code, 200)
            self.assertDictEqual(response.json(), {'transactionStatus': 'Rollback Complete'})

    def test_data_api_exception_handler(self):
        response = client.post("/BeginTransaction", json={'resourceArn': 'abc', 'secretArn': '1'})
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(response.json(),
                             {'code': 'BadRequestException', 'message': 'HttpEndPoint is not enabled for abc'})

    def test_execute_statement(self):
        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'
        meta = ResourceMeta(MySQL, lambda: None, 'localhost', 3306, 'test', 'pw')
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = 1, 1, 1, 1, 1, 1, 1,
        cursor_mock.fetchall.side_effect = [((1, 'abc'),)]

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret, \
                mock.patch('local_data_api.resources.resource.create_connection') as mock_create_connection:
            mock_get_secret.return_value = secret
            mock_create_connection.return_value = connection_mock
            response = client.post("/Execute",
                                   json={'resourceArn': 'abc', 'secretArn': '1', 'sql': 'select * from users'})
            self.assertEqual(response.status_code, 200)
            response_json = response.json()
            self.assertDictEqual(response_json,
                                 {'numberOfRecordsUpdated': 0,
                                  'records': [[{'longValue': 1}, {'stringValue': 'abc'}]]})

    def test_execute_statement_with_parameters(self):
        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'
        meta = ResourceMeta(MySQL, lambda: None, 'localhost', 3306, 'test', 'pw')
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = 1, 1, 1, 1, 1, 1, 1,
        cursor_mock.fetchall.side_effect = [((1, 'abc'),)]

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret, \
                mock.patch('local_data_api.resources.resource.create_connection') as mock_create_connection:
            mock_get_secret.return_value = secret
            mock_create_connection.return_value = connection_mock
            response = client.post("/Execute",
                                   json={'resourceArn': 'abc', 'secretArn': '1',
                                         'sql': 'select * from users where (:id)',
                                         'parameters': [{'name': 'id', 'value': {'longValue': 1}}]})
            self.assertEqual(response.status_code, 200)
            response_json = response.json()
            self.assertDictEqual(response_json,
                                 {'numberOfRecordsUpdated': 0,
                                  'records': [[{'longValue': 1}, {'stringValue': 'abc'}]]})

    def test_execute_statement_with_transaction(self):
        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'
        meta = ResourceMeta(MySQL, lambda: None, 'localhost', 3306, 'test', 'pw')
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = 1, 1, 1, 1, 1, 1, 1,
        cursor_mock.fetchall.side_effect = [((1, 'abc'),)]

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret, \
                mock.patch('local_data_api.resources.resource.CONNECTION_POOL', {'2': connection_mock}):
            mock_get_secret.return_value = secret
            response = client.post("/Execute",
                                   json={'resourceArn': 'abc', 'secretArn': '1', 'sql': 'select * from users',
                                         'transactionId': '2'})
            self.assertEqual(response.status_code, 200)
            response_json = response.json()
            self.assertDictEqual(response_json,
                                 {'numberOfRecordsUpdated': 0,
                                  'records': [[{'longValue': 1}, {'stringValue': 'abc'}]]})

    def test_batch_execute_statement(self):
        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'
        meta = ResourceMeta(MySQL, lambda: None, 'localhost', 3306, 'test', 'pw')
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = ''
        cursor_mock.rowcount = 1
        cursor_mock.lastrowid = 0

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret, \
                mock.patch('local_data_api.resources.resource.create_connection') as mock_create_connection:
            mock_get_secret.return_value = secret
            mock_create_connection.return_value = connection_mock
            response = client.post("/BatchExecute",
                                   json={'resourceArn': 'abc', 'secretArn': '1',
                                         'sql': "insert into users values (1, 'abc')"})
            self.assertEqual(response.status_code, 200)
            response_json = response.json()
            self.assertDictEqual(response_json,
                                 {'updateResults': []})

    def test_batch_execute_statement_with_parameters(self):
        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'
        meta = ResourceMeta(MySQL, lambda: None, 'localhost', 3306, 'test', 'pw')
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = ''
        cursor_mock.rowcount = 1
        cursor_mock.lastrowid = 0

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret, \
                mock.patch('local_data_api.resources.resource.create_connection') as mock_create_connection:
            mock_get_secret.return_value = secret
            mock_create_connection.return_value = connection_mock
            response = client.post("/BatchExecute",
                                   json={'resourceArn': 'abc', 'secretArn': '1',
                                         'sql': "insert into users values (:id, :name)",
                                         'parameterSets': [[
                                             {'name': 'id', 'value': {'longValue': 1}},
                                             {'name': 'name', 'value': {'stringValue': 'abc'}}]]})
            self.assertEqual(response.status_code, 200)
            response_json = response.json()
            self.assertDictEqual(response_json, {'updateResults': []})

    def test_batch_execute_statement_with_transaction(self):
        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'
        meta = ResourceMeta(MySQL, lambda: None, 'localhost', 3306, 'test', 'pw')
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = ''
        cursor_mock.rowcount = 1
        cursor_mock.lastrowid = 1

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret, \
                mock.patch('local_data_api.resources.resource.CONNECTION_POOL', {'2': connection_mock}):
            mock_get_secret.return_value = secret
            response = client.post("/BatchExecute",
                                   json={'resourceArn': 'abc', 'secretArn': '1',
                                         'sql': "insert into users (name) values (:name)",
                                         'transactionId': '2',
                                         'parameterSets': [[{'name': 'name', 'value': {'stringValue': 'abc'}}]]})

            self.assertEqual(response.status_code, 200)
            response_json = response.json()
            self.assertDictEqual(response_json, {'updateResults': [{'generatedFields': [{'longValue': 1}]}]})
