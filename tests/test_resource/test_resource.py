
from typing import Dict, Union
from unittest import TestCase, mock
from unittest.mock import Mock

from sqlalchemy.orm import sessionmaker

from local_data_api.exceptions import BadRequestException
from local_data_api.resources import MySQL
from local_data_api.resources.sqlite import SQLite
from local_data_api.resources.resource import get_resource, register_resource, RESOURCE_METAS, ResourceMeta

DATABASE_SETTINGS: Dict[str, Dict[str, Union[str, int]]] = {
    'SQLite': {
        'host': '',
        'port': None,
        'user_name': None,
        'password': None
         }
}


class TestDataBaseFunction(TestCase):
    def setUp(self) -> None:
        RESOURCE_METAS.clear()

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

    def test_get_database_invalid_resource_arn(self) -> None:
        resource_arn: str = 'dummy_resource_arn'

        engine = MySQL.create_engine('localhost', 3306, 'test', 'pw', {})

        session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

        RESOURCE_METAS[resource_arn] = ResourceMeta(SQLite, session, 'localhost', 3306, 'test', 'pw')

        with self.assertRaises(BadRequestException):
            get_resource('invalid_arn', 'secret_arn')
