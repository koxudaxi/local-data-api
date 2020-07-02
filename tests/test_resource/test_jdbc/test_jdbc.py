from __future__ import annotations

from base64 import b64encode
from typing import Any, Optional

import jaydebeapi
import pytest

from local_data_api.models import ColumnMetadata, Field
from local_data_api.resources.jdbc import JDBC, attach_thread_to_jvm, connection_maker
from local_data_api.resources.resource import JDBCType


class DummyJDBC(JDBC):
    def get_filed_from_jdbc_type(self, value: Any, jdbc_type: Optional[int]) -> Field:
        return super().get_filed_from_jdbc_type(value, jdbc_type)

    def autocommit_off(self, cursor: jaydebeapi.Cursor) -> None:
        pass

    JDBC_NAME = 'jdbc:dummy'
    DRIVER = 'dummy'

    @staticmethod
    def reset_generated_id(cursor: jaydebeapi.Cursor) -> None:
        cursor.execute('SELECT LAST_INSERT_ID(NULL)')

    @staticmethod
    def last_generated_id(cursor: jaydebeapi.Cursor) -> int:
        cursor.execute("SELECT LAST_INSERT_ID()")
        return int(str(cursor.fetchone()[0]))


def test_attach_thread_to_jvm(mocker):
    mock_jpype = mocker.Mock()
    mock_jpype.isJVMStarted.return_value = True
    mock_jpype.isThreadAttachedToJVM.return_value = False

    mock_jpype.java.lang.ClassLoader.getSystemClassLoader.return_value = 'abc'
    mock_current_thread = mocker.Mock()
    mock_jpype.java.lang.Thread.currentThread.return_value = mock_current_thread
    mocker.patch.dict('sys.modules', jpype=mock_jpype)
    attach_thread_to_jvm()

    mock_jpype.attachThreadToJVM.assert_called_once_with()
    mock_jpype.java.lang.ClassLoader.getSystemClassLoader.assert_called_once_with()
    mock_jpype.java.lang.Thread.currentThread.assert_called_once_with()
    mock_current_thread.setContextClassLoader.assert_called_once_with('abc')


def test_connection(mocker):
    mocker.patch('local_data_api.resources.jdbc.attach_thread_to_jvm')
    mock_jaydebeapi = mocker.patch('local_data_api.resources.jdbc.jaydebeapi')
    connection = connection_maker(
        jclassname='jdbc:db',
        url='localhost',
        driver_args={'user': 'root'},
        jars='test.jar',
        libs='lib.so',
    )
    connection()
    mock_jaydebeapi.connect.assert_called_once_with(
        'jdbc:db', 'localhost', {'user': 'root'}, 'test.jar', 'lib.so'
    )


def test_create_column_metadata_set(mocker):
    mock_meta = mocker.Mock()
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
    mock_cursor = mocker.Mock()
    mock_cursor._meta = mock_meta
    assert DummyJDBC(mocker.Mock()).create_column_metadata_set(mock_cursor) == [
        ColumnMetadata(
            arrayBaseColumnType=0,
            isAutoIncrement=True,
            isCaseSensitive=False,
            isCurrency=True,
            isSigned=True,
            label='a',
            name='c',
            nullable=0,
            precision=1,
            scale=3,
            schema='e',
            tableName='g',
            type=5,
            typeName='i',
        ),
        ColumnMetadata(
            arrayBaseColumnType=0,
            isAutoIncrement=False,
            isCaseSensitive=True,
            isCurrency=False,
            isSigned=False,
            label='b',
            name='d',
            nullable=1,
            precision=2,
            scale=4,
            schema='f',
            tableName='h',
            type=6,
            typeName='j',
        ),
    ]


def test_init(mocker):
    mock_attach_thread_to_jvm = mocker.patch(
        'local_data_api.resources.jdbc.attach_thread_to_jvm'
    )
    DummyJDBC(None)
    mock_attach_thread_to_jvm.assert_not_called()

    mock_attach_thread_to_jvm = mocker.patch(
        'local_data_api.resources.jdbc.attach_thread_to_jvm'
    )
    DummyJDBC(None, 'abc')
    mock_attach_thread_to_jvm.assert_called_once_with()


def test_create_connection_maker(mocker):
    mock_connect = mocker.patch('local_data_api.resources.jdbc.connection_maker')
    connection_maker = DummyJDBC.create_connection_maker(
        host='127.0.0.1',
        port=3306,
        user_name='root',
        password='pass',
        engine_kwargs={'JAR_PATH': 'test.jar'},
    )
    connection_maker()
    mock_connect.assert_called_once_with(
        'dummy',
        'jdbc:dummy://127.0.0.1:3306/',
        {'user': 'root', 'password': 'pass'},
        'test.jar',
    )


def test_create_connection_maker_error(mocker):
    mocker.patch('local_data_api.resources.jdbc.connection_maker')
    with pytest.raises(Exception) as e:
        DummyJDBC.create_connection_maker(
            host='127.0.0.1', port=3306, user_name='root', password='pass'
        )
    assert e.value.args[0] == 'Not Found JAR_PATH in settings'


@pytest.mark.parametrize(
    'value, jdbc_type, expected',
    [
        (1, JDBCType.INTEGER, {'longValue': 1}),
        (1, JDBCType.INTEGER, {'longValue': 1}),
        (0.1, JDBCType.FLOAT, {'doubleValue': 0.1}),
        (1, JDBCType.DECIMAL, {'stringValue': '1'}),
        (1, JDBCType.BOOLEAN, {'booleanValue': True}),
        (b'bytes', JDBCType.BLOB, {'blobValue': b64encode(b'bytes').decode()}),
        (1, JDBCType.REF, {'longValue': 1}),
        (1, -99999999999, {'longValue': 1}),
    ],
)
def test_get_filed_from_jdbc_type(value, jdbc_type, expected):
    assert (
        DummyJDBC(None)
        .get_filed_from_jdbc_type(
            value, jdbc_type.value if isinstance(jdbc_type, JDBCType) else jdbc_type
        )
        .dict(exclude_unset=True)
        == expected
    )
