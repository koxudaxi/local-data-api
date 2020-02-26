import datetime
from base64 import b64encode
from decimal import Decimal

import pytest
from local_data_api.models import Field, SqlParameter


def test_valid_field() -> None:
    assert SqlParameter(name='abc', value=Field(stringValue='abc')).valid_value == 'abc'
    assert SqlParameter(name='abc', value=Field(blobValue='abc')).valid_value == 'abc'
    assert SqlParameter(name='abc', value=Field(doubleValue=0.1)).valid_value == 0.1
    assert SqlParameter(name='abc', value=Field(isNull=True)).valid_value is None
    assert SqlParameter(name='abc', value=Field(longValue=123)).valid_value == 123
    assert SqlParameter(name='abc', value=Field(longValue=123)).valid_value == 123
    assert SqlParameter(name='abc', value=Field()).valid_value is None
    assert SqlParameter(
        name='abc', value=Field(stringValue='123456789'), typeHint='DECIMAL'
    ).valid_value == Decimal(123456789)
    assert SqlParameter(
        name='abc',
        value=Field(stringValue='2020-02-27 00:30:15.290'),
        typeHint='TIMESTAMP',
    ).valid_value == datetime.datetime(2020, 2, 27, 0, 30, 15, 290000)
    assert SqlParameter(
        name='abc', value=Field(stringValue='00:30:15.290'), typeHint='TIME'
    ).valid_value == datetime.time(0, 30, 15, 290000)
    assert SqlParameter(
        name='abc', value=Field(stringValue='2020-02-27'), typeHint='DATE'
    ).valid_value == datetime.date(2020, 2, 27)


def test_from_value() -> None:
    assert Field.from_value('str') == Field(stringValue='str')
    assert Field.from_value(123) == Field(longValue=123)
    assert Field.from_value(1.23) == Field(doubleValue=1.23)
    assert Field.from_value(True) == Field(booleanValue=True)
    assert Field.from_value(False) == Field(booleanValue=False)
    assert Field.from_value(b'bytes') == Field(blobValue=b64encode(b'bytes'))
    assert Field.from_value(None) == Field(isNull=True)

    class JavaUUID:
        def __init__(self, val: str):
            self._val: str = val

        def __str__(self) -> str:
            return self._val

    uuid = 'e9e1df6b-c6d3-4a34-9227-c27056d596c6'
    assert Field.from_value(JavaUUID(uuid)) == Field(stringValue=uuid)

    class PGobject:
        def __init__(self, val: str):
            self._val: str = val

        def __str__(self) -> str:
            return self._val

    assert Field.from_value(PGobject("{}")) == Field(stringValue="{}")

    class Dummy:
        pass

    with pytest.raises(Exception):
        Field.from_value(Dummy())
