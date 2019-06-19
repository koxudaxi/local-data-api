
from typing import Dict, Union
from unittest import TestCase, mock
from unittest.mock import Mock

from sqlalchemy.orm import sessionmaker, Session

from local_data_api.exceptions import BadRequestException, InternalServerErrorException
from local_data_api.resources import MySQL
from local_data_api.resources.sqlite import SQLite
from local_data_api.resources.resource import get_resource, register_resource, RESOURCE_METAS, ResourceMeta, \
    SESSION_POOL, set_session, get_session, delete_session, get_resource_class, create_resource_arn

DATABASE_SETTINGS: Dict[str, Dict[str, Union[str, int]]] = {
    'SQLite': {
        'host': '',
        'port': None,
        'user_name': None,
        'password': None
         }
}


class TestResourceFunction(TestCase):
    def setUp(self) -> None:
        RESOURCE_METAS.clear()
        SESSION_POOL.clear()

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

        engine = MySQL.create_engine('localhost', 3306, 'test', 'pw', {})

        session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

        RESOURCE_METAS[resource_arn] = ResourceMeta(SQLite, session, 'localhost', 3306, 'test', 'pw')

        secret = Mock()
        secret.user_name = 'test'
        secret.password = 'pw'

        with mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret:
            mock_get_secret.return_value = secret
            resource = get_resource(resource_arn, 'dummy')
            self.assertIsInstance(resource, SQLite)
            with mock.patch('local_data_api.resources.resource.get_session') as mock_get_session:
                new_session = Session()
                mock_get_session.return_value = new_session
                resource = get_resource(resource_arn, 'dummy', 'transaction')
            self.assertEqual(resource.session, new_session)

    def test_get_resource_exception(self) -> None:

        resource_arn: str = 'dummy_resource_arn'

        engine = MySQL.create_engine('localhost', 3306, 'test', 'pw', {})

        session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

        RESOURCE_METAS[resource_arn] = ResourceMeta(SQLite, session, 'localhost', 3306, 'test', 'pw')

        with self.assertRaises(BadRequestException):
            get_resource('invalid', 'dummy')

        with self.assertRaises(InternalServerErrorException):
            SESSION_POOL['dummy'] = Session()
            get_resource('invalid', 'dummy', 'dummy')
            del SESSION_POOL['dummy']

        with mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret:
            with self.assertRaises(BadRequestException):
                mock_get_secret.side_effect=BadRequestException('error')
                get_resource(resource_arn, 'dummy')

            with self.assertRaises(InternalServerErrorException):
                mock_get_secret.side_effect = BadRequestException('error')
                SESSION_POOL['dummy'] = Session()
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

        engine = MySQL.create_engine('localhost', 3306, 'test', 'pw', {})

        session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

        RESOURCE_METAS[resource_arn] = ResourceMeta(SQLite, session, 'localhost', 3306, 'test', 'pw')

        with self.assertRaises(BadRequestException):
            get_resource('invalid_arn', 'secret_arn')


class TestSessionPool(TestCase):
    def setUp(self) -> None:
        SESSION_POOL.clear()

    def test_set_session(self) -> None:
        session: Session = Session()
        set_session('abc', session)
        self.assertEqual(SESSION_POOL['abc'], session)

    def test_get_session(self) -> None:
        session: Session = Session()
        SESSION_POOL['abc'] = session
        self.assertEqual(get_session('abc'), session)

    def test_get_session_notfound(self) -> None:
        with self.assertRaises(BadRequestException):
            get_session('abc')

    def test_delete_session(self) -> None:
        session: Session = Session()
        SESSION_POOL['abc'] = session
        delete_session()
        self.assertEqual(SESSION_POOL['abc'], session)

        delete_session('abc')
        self.assertTrue('abc' not in SESSION_POOL)

