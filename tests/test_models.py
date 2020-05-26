import datetime
from base64 import b64encode
from datetime import datetime

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

    assert (
        SqlParameter(
            name='abc', value=Field(stringValue='123456789'), typeHint='DECIMAL'
        ).valid_value
        == '123456789'
    )
    assert (
        SqlParameter(
            name='abc',
            value=Field(stringValue='2020-02-27 00:30:15.290'),
            typeHint='TIMESTAMP',
        ).valid_value
        == '2020-02-27 00:30:15.290'
    )
    assert (
        SqlParameter(
            name='abc', value=Field(stringValue='00:30:15.290'), typeHint='TIME'
        ).valid_value
        == '00:30:15.290'
    )
    assert (
        SqlParameter(
            name='abc', value=Field(stringValue='2020-02-27'), typeHint='DATE'
        ).valid_value
        == '2020-02-27'
    )
