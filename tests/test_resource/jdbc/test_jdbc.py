from typing import Dict, List
from unittest import TestCase, mock

from local_data_api.models import ColumnMetadata
from local_data_api.resources.jdbc import JDBC, connection_maker, create_column_metadata_set


class Metadata:
    def __init__(self, data: List[Dict]):
        self._data: List[Dict] = data

    def isAutoIncrement(self, index):
        return self._data[index]['isAutoIncrement']

    def isCaseSensitive(self, index):
        return self._data[index]['isCaseSensitive']

    def isCurrency(self, index):
        return self._data[index]['isCurrency']

    def isSigned(self, index):
        return self._data[index]['isSigned']

    def getColumnLabel(self, index):
        return self._data[index]['getColumnLabel']

    def getColumnName(self, index):
        return self._data[index]['getColumnName']

    def getPrecision(self, index):
        return self._data[index]['getPrecision']

    def isNullable(self, index):
        return self._data[index]['isNullable']

    def getScale(self, index):
        return self._data[index]['getScale']

    def getSchemaName(self, index):
        return self._data[index]['getSchemaName']

    def getTableName(self, index):
        return self._data[index]['getTableName']

    def getColumnType(self, index):
        return self._data[index]['getColumnType']

    def getColumnTypeName(self, index):
        return self._data[index]['getColumnTypeName']

    def getColumnCount(self):
        return len(self._data)


class TestJDBCFunction(TestCase):

    def test_create_column_metadata_set(self) -> None:
        meta = Metadata([{
            'isAutoIncrement': True,
            'isCaseSensitive': False,
            'isCurrency': True,
            'isSigned': False,
            'getColumnLabel': 'columnLabel1',
            'getColumnName': 'columnName1',
            'getPrecision': 1,
            'isNullable': True,
            'getScale': 2,
            'getSchemaName': 'schemaName1',
            'getTableName': 'tableName1',
            'getColumnType': 3,
            'getColumnTypeName': 'columnTypeName1'},
            {
                'isAutoIncrement': False,
                'isCaseSensitive': True,
                'isCurrency': False,
                'isSigned': True,
                'getColumnLabel': 'columnLabel2',
                'getColumnName': 'columnName2',
                'getPrecision': 4,
                'isNullable': False,
                'getScale': 5,
                'getSchemaName': 'schemaName2',
                'getTableName': 'tableName2',
                'getColumnType': 6,
                'getColumnTypeName': 'columnTypeName2'}
        ])
        self.assertEqual(create_column_metadata_set(meta)[0],
                         ColumnMetadata(**{'arrayBaseColumnType': 0,
                                           'isAutoIncrement': True,
                                           'isCaseSensitive': False,
                                           'isCurrency': True,
                                           'isSigned': False,
                                           'label': 'columnLabel1',
                                           'name': 'columnName1',
                                           'nullable': None,
                                           'precision': 1,
                                           'scale': 2,
                                           'schema_': None,
                                           'tableName': 'tableName1',
                                           'type': 3,
                                           'typeName': 'columnTypeName1'}))

        self.assertEqual(create_column_metadata_set(meta)[1],
                         ColumnMetadata(**{'arrayBaseColumnType': 0,
                                           'isAutoIncrement': False,
                                           'isCaseSensitive': True,
                                           'isCurrency': False,
                                           'isSigned': True,
                                           'label': 'columnLabel2',
                                           'name': 'columnName2',
                                           'nullable': None,
                                           'precision': 4,
                                           'scale': 5,
                                           'schema_': None,
                                           'tableName': 'tableName2',
                                           'type': 6,
                                           'typeName': 'columnTypeName2'}))

    def test_connection_maker(self):
        with mock.patch('jaydebeapi.connect') as mock_connect:
            connection = connection_maker('jdbc:mariadb', 'db', {'username': 'admin'}, '/tmp/db.jar', '/libs/lib')
            self.assertFalse(mock_connect.called)
            connection()
            mock_connect.assert_called_once_with('jdbc:mariadb', 'db', {'username': 'admin'}, '/tmp/db.jar',
                                                 '/libs/lib')


class TestJDBC(TestCase):
    def test_create_connection_maker(self):
        class Dummy(JDBC):
            DRIVER = 'org.dummy.jdbc.Driver'
            JDBC_NAME = 'dummy'
            pass

        with mock.patch('local_data_api.resources.jdbc.connection_maker') as connection_maker_mock:
            Dummy.create_connection_maker('db', 3306, 'user', 'pass', {'JAR_PATH': '/tmp/db.jar'})
            connection_maker_mock.assert_called_once_with('org.dummy.jdbc.Driver', 'dummy://db:3306',
                                                          {'user': 'user', 'password': 'pass'}, '/tmp/db.jar')
