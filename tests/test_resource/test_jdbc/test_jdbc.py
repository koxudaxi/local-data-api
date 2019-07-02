from __future__ import annotations

from unittest import TestCase

from mock import patch, Mock

from local_data_api.models import ColumnMetadata
from local_data_api.resources.jdbc import connection_maker, attach_thread_to_jvm, JDBC, create_column_metadata_set


class DummyJDBC(JDBC):
    JDBC_NAME = 'jdbc:dummy'
    DRIVER = 'dummy'


class TestJDBCFunction(TestCase):
    def test_attach_thread_to_jvm(self):
        mock_jpype = Mock()
        mock_jpype.isJVMStarted.return_value = True
        mock_jpype.isThreadAttachedToJVM.return_value = False

        mock_jpype.java.lang.ClassLoader.getSystemClassLoader.return_value = 'abc'
        mock_current_thread = Mock()
        mock_jpype.java.lang.Thread.currentThread.return_value = mock_current_thread
        with patch.dict('sys.modules', jpype=mock_jpype):
            attach_thread_to_jvm()

            mock_jpype.attachThreadToJVM.assert_called_once_with()
            mock_jpype.java.lang.ClassLoader.getSystemClassLoader.assert_called_once_with()
            mock_jpype.java.lang.Thread.currentThread.assert_called_once_with()
            mock_current_thread.setContextClassLoader.assert_called_once_with('abc')

    def test_connection(self):
        with patch('local_data_api.resources.jdbc.attach_thread_to_jvm'), \
             patch('local_data_api.resources.jdbc.jaydebeapi') as mock_jaydebeapi:
            connection = connection_maker(jclassname='jdbc:db', url='localhost', driver_args={'user': 'root'},
                                          jars='test.jar', libs='lib.so')
            connection()
            mock_jaydebeapi.connect.assert_called_once_with('jdbc:db', 'localhost', {'user': 'root'},
                                                            'test.jar', 'lib.so')

    def test_create_column_metadata_set(self):
        mock_meta = Mock()
        mock_meta.isAutoIncrement.side_effect = [True, False]
        mock_meta.isCaseSensitive.side_effect = [False, True]
        mock_meta.isCurrency.side_effect = [True, False]
        mock_meta.isSigned.side_effect = [True, False]
        mock_meta.getColumnLabel.side_effect = ['a', 'b']
        mock_meta.getColumnName.side_effect = ['c', 'd']
        mock_meta.isNullable.side_effect = [0, 1]
        mock_meta.getPrecision.side_effect = [1, 2]
        mock_meta.getScale.side_effect = [3, 4]
        mock_meta.getSchemaName.side_effect = ['e', 'f']
        mock_meta.getTableName.side_effect = ['g', 'h']
        mock_meta.getColumnType.side_effect = [5, 6]
        mock_meta.getColumnTypeName.side_effect = ['i', 'j']
        mock_meta.getColumnCount.return_value = 2

        self.assertListEqual(create_column_metadata_set(mock_meta),
                             [ColumnMetadata(arrayBaseColumnType=0,
                                             isAutoIncrement=True,
                                             isCaseSensitive=False,
                                             isCurrency=True,
                                             isSigned=True,
                                             label='a',
                                             name='c',
                                             nullabl=0,
                                             precision=1,
                                             scale=3,
                                             schema_='e',
                                             tableName='g',
                                             type=5,
                                             typeName='i'),
                              ColumnMetadata(arrayBaseColumnType=0,
                                             isAutoIncrement=False,
                                             isCaseSensitive=True,
                                             isCurrency=False,
                                             isSigned=False,
                                             label='b',
                                             name='d',
                                             nullabl=0,
                                             precision=2,
                                             scale=4,
                                             schema_='e',
                                             tableName='h',
                                             type=6,
                                             typeName='j')])


class TestJDBC(TestCase):
    def test_init(self):
        with patch('local_data_api.resources.jdbc.attach_thread_to_jvm') as mock_attach_thread_to_jvm:
            DummyJDBC(None)
            mock_attach_thread_to_jvm.assert_not_called()

        with patch('local_data_api.resources.jdbc.attach_thread_to_jvm') as mock_attach_thread_to_jvm:
            DummyJDBC(None, 'abc')
            mock_attach_thread_to_jvm.assert_called_once_with()

    def test_create_connection_maker(self):
        with patch('local_data_api.resources.jdbc.connection_maker') as mock_connect:
            connection_maker = DummyJDBC.create_connection_maker(host='127.0.0.1', port=3306, user_name='root',
                                                                 password='pass',
                                                                 engine_kwargs={'JAR_PATH': 'test.jar'})
            connection_maker()
            mock_connect.assert_called_once_with('dummy', 'jdbc:dummy://127.0.0.1:3306',
                                                 {'user': 'root', 'password': 'pass'}, 'test.jar')

    def test_create_connection_maker_error(self):
        with patch('local_data_api.resources.jdbc.connection_maker'):
            with self.assertRaises(Exception) as e:
                DummyJDBC.create_connection_maker(host='127.0.0.1', port=3306, user_name='root', password='pass')
            self.assertEqual(e.exception.args[0], 'Not Found JAR_PATH in settings')
