
from typing import Dict, Union
from unittest import TestCase, mock
from unittest.mock import Mock

from local_data_api.exceptions import BadRequestException
from local_data_api.resources.sqlite import SQLite
from local_data_api.resources.resource import create_resource, RESOURCE_POOL, Resource, get_resource

DATABASE_SETTINGS: Dict[str, Dict[str, Union[str, int]]] = {
    'SQLite': {
        'host': '',
        'port': None,
        'user_name': None,
        'password': None
         }
}


class DataBaseFunction(TestCase):
    def setUp(self) -> None:
        RESOURCE_POOL.clear()

    def test_create_resource(self) -> None:
        resource_arn: str = 'dummy_resource_arn'
        result_arn = create_resource('MySQL', host='localhost', port=3306, user_name='test', password='pw',
                                     resource_arn='dummy_resource_arn')
        self.assertEqual(resource_arn, result_arn)

        database: Resource = RESOURCE_POOL[resource_arn]
        self.assertEqual(database.host, 'localhost')
        self.assertEqual(database.port, 3306)
        self.assertEqual(database.user_name, 'test')
        self.assertEqual(database.password, 'pw')
        self.assertTrue(isinstance(database, Resource))

    def test_create_resource_without_arn(self) -> None:
        result_arn = create_resource('MySQL', host='localhost', port=3306, user_name='test', password='pw',
                                     )

        database: Resource = RESOURCE_POOL[result_arn]
        self.assertEqual(database.host, 'localhost')
        self.assertEqual(database.port, 3306)
        self.assertEqual(database.user_name, 'test')
        self.assertEqual(database.password, 'pw')
        self.assertTrue(isinstance(database, Resource))

    def test_get_resource(self) -> None:
        resource_arn: str = 'dummy_resource_arn'

        database: SQLite = SQLite()
        RESOURCE_POOL[resource_arn] = database
        secret = Mock()
        secret.user_name = None
        secret.password = None

        with mock.patch('local_data_api.resources.resource.get_secret') as mock_get_secret:
            mock_get_secret.return_value = secret
            self.assertEqual(get_resource(resource_arn, 'dummy'), database)

    def test_get_database_invalid_resource_arn(self) -> None:
        resource_arn: str = 'dummy_resource_arn'
        secret_arn: str = 'dummy_secret_arn'
        database: SQLite = SQLite()
        RESOURCE_POOL[resource_arn] = database
        with self.assertRaises(BadRequestException):
            get_resource('invalid_arn', secret_arn)
