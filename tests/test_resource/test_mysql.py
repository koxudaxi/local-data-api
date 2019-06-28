from __future__ import annotations

import re
from typing import Any, Dict, Optional, TYPE_CHECKING, Union
from unittest import TestCase, mock
from unittest.mock import Mock

from sqlalchemy.dialects import mysql

from local_data_api import convert_value
from local_data_api.exceptions import BadRequestException, InternalServerErrorException
from local_data_api.models import ColumnMetadata, ExecuteStatementResponse, Field
from local_data_api.resources import SQLite
from local_data_api.resources.resource import CONNECTION_POOL, RESOURCE_METAS, Resource, ResourceMeta, \
    create_column_metadata, create_resource_arn, get_resource, get_resource_class, register_resource

DATABASE_SETTINGS: Dict[str, Dict[str, Union[str, int]]] = {
    'SQLite': {
        'host': '',
        'port': None,
        'user_name': None,
        'password': None
    }
}

if TYPE_CHECKING:
    from local_data_api.resources.resource import ConnectionMaker


class DummyResource(Resource):
    DIALECT = mysql.dialect(paramstyle='named')

    @classmethod
    def create_connection_maker(cls, host: Optional[str] = None, port: Optional[int] = None,
                                user_name: Optional[str] = None, password: Optional[str] = None,
                                engine_kwargs: Dict[str, Any] = None) -> ConnectionMaker:
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
                mock_get_secret.side_effect = BadRequestException('error')
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

    def test_create_column_metadata(self):
        self.assertEqual(create_column_metadata('name', 1, 2, 3, 4, 5, True),
                         ColumnMetadata(arrayBaseColumnType=0, isAutoIncrement=False, isCaseSensitive=False,
                                        isCurrency=False, isSigned=False, label='name', name='name', nullable=None,
                                        precision=4, scale=5, tableName=None, type=None, typeName=None, schema_=None)
                         )


class TestResource(TestCase):
    def test_create_query(self):
        query = DummyResource.create_query('insert into users values (:id, :name)', {'id': 1, 'name': 'abc'})
        self.assertEqual(query, "insert into users values (1, 'abc')")

    def test_transaction_id(self):
        connection_mock = Mock()
        dummy = DummyResource(connection_mock, transaction_id='123')
        self.assertEqual(dummy.transaction_id, '123')

    def test_connection(self):
        connection_mock = Mock()
        dummy = DummyResource(connection_mock)
        self.assertEqual(dummy.connection, connection_mock)

    def test_create_transaction_id(self):
        transaction_id: str = DummyResource.create_transaction_id()
        self.assertTrue(re.match(r'[abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ/=+]{184}$', transaction_id))

    def test_commit(self):
        connection_mock = Mock()
        dummy = DummyResource(connection_mock)
        dummy.commit()
        self.assertEqual(connection_mock.commit.call_count, 1)

    def test_rollback(self):
        connection_mock = Mock()
        dummy = DummyResource(connection_mock)
        dummy.rollback()
        self.assertEqual(connection_mock.rollback.call_count, 1)

    def test_use_database(self):
        execute_mock = Mock()
        dummy = DummyResource(Mock())
        dummy.execute = execute_mock
        dummy.use_database('abc')
        execute_mock.assert_called_once_with('use abc')

    def test_begin(self):
        create_transaction_id_mock = Mock(side_effect=['abc'])
        connection_mock = Mock()
        dummy = DummyResource(connection_mock)
        dummy.create_transaction_id = create_transaction_id_mock
        with mock.patch('local_data_api.resources.resource.set_connection') as set_connection_mock:
            result = dummy.begin()
            self.assertEqual(result, 'abc')
            set_connection_mock.assert_called_once_with('abc', connection_mock)

    def test_close(self):
        connection_mock = Mock()
        dummy = DummyResource(connection_mock, 'abc')
        with mock.patch('local_data_api.resources.resource.delete_connection') as delete_connection_mock, \
                mock.patch('local_data_api.resources.resource.CONNECTION_POOL', {'abc': ''}):
            dummy.close()
            connection_mock.close.assert_called_once_with()
            delete_connection_mock.assert_called_once_with('abc')

    def test_execute_insert(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = ''
        cursor_mock.rowcount = 1
        cursor_mock.lastrowid = 1
        dummy = DummyResource(connection_mock)
        self.assertEqual(dummy.execute("insert into users values (1, 'abc')"),
                         ExecuteStatementResponse(numberOfRecordsUpdated=1, generatedFields=[Field(longValue=1)]))
        cursor_mock.execute.assert_called_once_with("insert into users values (1, 'abc')")
        cursor_mock.close.assert_called_once_with()

    def test_execute_insert_with_params(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = ''
        cursor_mock.rowcount = 1
        cursor_mock.lastrowid = 1
        dummy = DummyResource(connection_mock)
        self.assertEqual(dummy.execute("insert into users values (:id, :name)", {'id': 1, 'name': 'abc'}),
                         ExecuteStatementResponse(numberOfRecordsUpdated=1, generatedFields=[Field(longValue=1)]))
        cursor_mock.execute.assert_called_once_with("insert into users values (1, 'abc')")
        cursor_mock.close.assert_called_once_with()

    def test_execute_select(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = 1, 1, 1, 1, 1, 1, 1,
        cursor_mock.fetchall.side_effect = [((1, 'abc'),)]
        dummy = DummyResource(connection_mock, transaction_id='123')
        dummy.use_database = Mock()
        self.assertEqual(dummy.execute("select * from users", database_name='test'),
                         ExecuteStatementResponse(numberOfRecordsUpdated=0,
                                                  records=[[convert_value(1), convert_value('abc')]]))
        cursor_mock.execute.assert_called_once_with('select * from users')
        cursor_mock.close.assert_called_once_with()

    def test_execute_select_with_include_metadata(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        connection_mock.cursor.side_effect = [cursor_mock]
        cursor_mock.description = (1, 2, 3, 4, 5, 6, 7,), (8, 9, 10, 11, 12, 13, 14,),
        cursor_mock.fetchall.side_effect = [((1, 'abc'),)]
        dummy = DummyResource(connection_mock, transaction_id='123')
        dummy.use_database = Mock()
        self.assertEqual(
            dummy.execute("select * from users", database_name='test', include_result_metadata=True).dict(),
            ExecuteStatementResponse(numberOfRecordsUpdated=0, records=[[convert_value(1), convert_value('abc')]],
                                     columnMetadata=[ColumnMetadata(arrayBaseColumnType=0,
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
                                                                    typeName=None),
                                                     ColumnMetadata(arrayBaseColumnType=0,
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
                                                                    typeName=None)]))

        cursor_mock.execute.assert_called_once_with('select * from users')
        cursor_mock.close.assert_called_once_with()

    def test_execute_exception_1(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        error = Exception('errro')
        error.orig = ['error_message']
        cursor_mock.execute.side_effect = error
        connection_mock.cursor.side_effect = [cursor_mock]
        dummy = DummyResource(connection_mock, transaction_id='123')
        with self.assertRaises(BadRequestException):
            dummy.execute("select * from users")
        cursor_mock.execute.assert_called_once_with('select * from users')
        cursor_mock.close.assert_called_once_with()

    def test_execute_exception_2(self):
        connection_mock = Mock()
        cursor_mock = Mock()
        error = Exception('errro')
        error_orig = Mock()
        error_orig.args = ['', 'error_message']
        error.orig = error_orig
        cursor_mock.execute.side_effect = error
        connection_mock.cursor.side_effect = [cursor_mock]
        dummy = DummyResource(connection_mock, transaction_id='123')
        with self.assertRaises(BadRequestException):
            dummy.execute("select * from users")
        cursor_mock.execute.assert_called_once_with('select * from users')
        cursor_mock.close.assert_called_once_with()
