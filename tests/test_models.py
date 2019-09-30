from base64 import b64encode

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


def test_from_value() -> None:
    assert Field.from_value('str') == Field(stringValue='str')
    assert Field.from_value(123) == Field(longValue=123)
    assert Field.from_value(1.23) == Field(doubleValue=1.23)
    assert Field.from_value(True) == Field(booleanValue=True)
    assert Field.from_value(False) == Field(booleanValue=False)
    assert Field.from_value(b'bytes') == Field(blobValue=b64encode(b'bytes'))
    assert Field.from_value(None) == Field(isNull=True)

    class Dummy:
        pass

    with pytest.raises(Exception):
        Field.from_value(Dummy())
