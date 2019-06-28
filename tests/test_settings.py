
from unittest import TestCase, mock

from local_data_api.settings import setup


class TestSettingsFunction(TestCase):
    def setUp(self) -> None:
        pass

    def test_setup(self) -> None:
        with mock.patch('local_data_api.settings.register_secret') as mock_register_secret, \
             mock.patch('local_data_api.settings.register_resource') as mock_register_resource:
            setup()
            mock_register_secret.assert_called_with('root', 'example', 'dummy')
            mock_register_resource.assert_called_with('dummy', 'MySQLJDBC', '127.0.0.1', 3306, 'root', 'example',
                                                      {'JAR_PATH': '/usr/lib/jvm/mariadb-java-client.jar'})
