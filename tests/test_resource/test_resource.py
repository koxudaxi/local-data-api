from __future__ import annotations

from typing import Dict, Union
from unittest import TestCase, mock
from unittest.mock import Mock

from local_data_api.exceptions import BadRequestException, InternalServerErrorException
from local_data_api.resources import SQLite
from local_data_api.resources.resource import get_resource, register_resource, RESOURCE_METAS, ResourceMeta, \
    CONNECTION_POOL, set_connection, get_connection, delete_connection, get_resource_class, create_resource_arn

DATABASE_SETTINGS: Dict[str, Dict[str, Union[str, int]]] = {
    'SQLite': {
        'host': '',
        'port': None,
        'user_name': None,
        'password': None
         }
}


class ConnectionMock(Mock):
    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass


class TestResourceFunction(TestCase):
    def setUp(self) -> None:
        RESOURCE_METAS.clear()
        CONNECTION_POOL.clear()

    def test_register_resource(self) -> None:
        resource_arn: str = 'dummy_resource_arn'
        register_resource(resource_arn, 'MySQL', host='localhost', port=3306, user_name='test', password='pw')

        resource_meta: ResourceMeta = RESOURCE_METAS[resource_arn]
        self.assertEqual(resource_meta.host, 'localhost')
        self.assertEqual(resource_meta.port, 3306)
        self.assertEqual(resource_meta.user_name, 'test')
        self.assertEqual(resource_meta.password, 'pw')

    def test_get_resource(self) -> None:

        resource_arn: str = 'dummy_resource_arn'

        connection_maker = SQLite.create_connection_maker('localhost', 3306, 'test', 'pw', {})

        RESOURCE_METAS[resource_arn] = ResourceMeta(SQLite, connection_maker, 'localhost', 3306, 'test', 'pw')

        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'

        with mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret:
            mock_get_secret.return_value = secret
            resource = get_resource(resource_arn, 'dummy')
            self.assertIsInstance(resource, SQLite)
            with mock.patch('local_data_api.resources.resource.get_connection') as mock_get_connection:
                new_connection = Mock()
                mock_get_connection.return_value = new_connection
                resource = get_resource(resource_arn, 'dummy', 'transaction')
            self.assertEqual(resource.connection, new_connection)

    def test_get_resource_exception(self) -> None:

        resource_arn: str = 'dummy_resource_arn'

        connection_maker = SQLite.create_connection_maker()

        RESOURCE_METAS[resource_arn] = ResourceMeta(SQLite, connection_maker, 'localhost', 3306, 'test', 'pw')

        with self.assertRaises(BadRequestException):
            get_resource('invalid', 'dummy')

        with self.assertRaises(InternalServerErrorException):
            CONNECTION_POOL['dummy'] = connection_maker()
            get_resource('invalid', 'dummy', 'dummy')
            del CONNECTION_POOL['dummy']

        with mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret:
            with self.assertRaises(BadRequestException):
                mock_get_secret.side_effect=BadRequestException('error')
                get_resource(resource_arn, 'dummy')

            with self.assertRaises(InternalServerErrorException):
                mock_get_secret.side_effect = BadRequestException('error')
                CONNECTION_POOL['dummy'] = connection_maker()
                get_resource(resource_arn, 'dummy', 'dummy')

            mock_get_secret.side_effect = None
            secret = Mock()
            secret.user_name = 'invalid'
            secret.password = 'pw'

            mock_get_secret.return_value = secret
            with self.assertRaises(BadRequestException):
                get_resource(resource_arn, 'dummy')

            secret = Mock()
            secret.user_name = 'test'
            secret.password = 'invalid'

            mock_get_secret.return_value = secret
            with self.assertRaises(BadRequestException):
                get_resource(resource_arn, 'dummy')

    def test_get_resource_class_exception(self) -> None:
        with self.assertRaises(Exception):
            get_resource_class('invalid_engine')

    def test_create_resource_arn(self):
        self.assertEqual(create_resource_arn('us-east-1', '123456789012')[:57],
                         'arn:aws:rds:us-east-1:123456789012:cluster:local-data-api')

    def test_get_database_invalid_resource_arn(self) -> None:
        resource_arn: str = 'dummy_resource_arn'

        connection_maker = SQLite.create_connection_maker('localhost', 3306, 'test', 'pw', {})

        RESOURCE_METAS[resource_arn] = ResourceMeta(SQLite, connection_maker, 'localhost', 3306, 'test', 'pw')

        with self.assertRaises(BadRequestException):
            get_resource('invalid_arn', 'secret_arn')


class TestconnectionPool(TestCase):
    def setUp(self) -> None:
        CONNECTION_POOL.clear()

    def test_set_connection(self) -> None:
        connection: ConnectionMock = ConnectionMock()
        set_connection('abc', connection)
        self.assertEqual(CONNECTION_POOL['abc'], connection)

    def test_get_connection(self) -> None:
        connection: ConnectionMock = ConnectionMock()
        CONNECTION_POOL['abc'] = connection
        self.assertEqual(get_connection('abc'), connection)

    def test_get_connection_notfound(self) -> None:
        with self.assertRaises(BadRequestException):
            get_connection('abc')

    def test_delete_connection(self) -> None:
        connection: ConnectionMock = ConnectionMock()
        CONNECTION_POOL['abc'] = connection

        delete_connection('abc')
        self.assertTrue('abc' not in CONNECTION_POOL)
