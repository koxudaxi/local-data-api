from __future__ import annotations

from unittest import TestCase
from unittest.mock import patch

from local_data_api.resources import MySQL


class TestMySQL(TestCase):
    def test_create_connection_maker(self):
        with patch('local_data_api.resources.mysql.pymysql.connect') as mock_connect:
            connection_maker = MySQL.create_connection_maker(host='127.0.0.1', port=3306, user_name='root',
                                                             password='pass', engine_kwargs={'auto_commit': True})
            connection_maker()
        mock_connect.assert_called_once_with(auto_commit=True, host='127.0.0.1', password='pass',
                                             port=3306, user='root')

        with patch('local_data_api.resources.mysql.pymysql.connect') as mock_connect:
            connection_maker = MySQL.create_connection_maker()
            connection_maker()
        mock_connect.assert_called_once_with()
