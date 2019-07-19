from unittest import TestCase, mock
from unittest.mock import Mock

from starlette.testclient import TestClient

from local_data_api.main import app
from local_data_api.resources import SQLite
from local_data_api.resources.resource import RESOURCE_METAS, CONNECTION_POOL, ResourceMeta
from tests.test_resource.test_resource import DummyResource

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
        meta =  ResourceMeta(SQLite, lambda : None, 'localhost', 3306, 'test', 'pw')

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret:
            mock_get_secret.return_value = secret
            response = client.post("/BeginTransaction",  json={'resourceArn': 'abc', 'secretArn': '1'})
            self.assertEqual(response.status_code, 200)
            response_json = response.json()
            self.assertEqual(len(response_json), 1)
            self.assertTrue('transactionId' in response_json)
            self.assertTrue(isinstance(response_json['transactionId'], str))

    def test_commit_transaction(self):
        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'
        meta =  ResourceMeta(SQLite, lambda : None, 'localhost', 3306, 'test', 'pw')
        connection_mock = Mock()

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret, \
             mock.patch('local_data_api.resources.resource.CONNECTION_POOL', {'2': connection_mock}):
            mock_get_secret.return_value = secret
            response = client.post("/CommitTransaction",  json={'resourceArn': 'abc', 'secretArn': '1',
                                                                'transactionId': '2'})
            self.assertEqual(response.status_code, 200)
            self.assertDictEqual(response.json(), {'transactionStatus': 'Transaction Committed'})

    def test_rollback_transaction(self):
        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'
        meta =  ResourceMeta(SQLite, lambda : None, 'localhost', 3306, 'test', 'pw')
        connection_mock = Mock()

        with mock.patch('local_data_api.resources.resource.RESOURCE_METAS', {'abc': meta}), \
             mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret, \
             mock.patch('local_data_api.resources.resource.CONNECTION_POOL', {'2': connection_mock}):
            mock_get_secret.return_value = secret
            response = client.post("/RollbackTransaction",  json={'resourceArn': 'abc', 'secretArn': '1',
                                                                'transactionId': '2'})
            self.assertEqual(response.status_code, 200)
            self.assertDictEqual(response.json(), {'transactionStatus': 'Rollback Complete'})


    def test_data_api_exception_handler(self):
        response = client.post("/BeginTransaction",  json={'resourceArn': 'abc', 'secretArn': '1'})
        self.assertEqual(response.status_code, 400)
        self.assertDictEqual(response.json(), {'code': 'BadRequestException',  'message': 'HttpEndPoint is not enabled for abc'})

