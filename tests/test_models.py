from base64 import b64encode
from unittest import TestCase

from local_data_api.models import Field, SqlParameter


class TestSqlParameter(TestCase):
    def test_valid_field(self) -> None:
        self.assertEqual(
            SqlParameter(name='abc', value=Field(stringValue='abc')).valid_value, 'abc'
        )
        self.assertEqual(
            SqlParameter(name='abc', value=Field(blobValue='abc')).valid_value, 'abc'
        )
        self.assertEqual(
            SqlParameter(name='abc', value=Field(doubleValue=0.1)).valid_value, 0.1
        )
        self.assertEqual(
            SqlParameter(name='abc', value=Field(isNull=True)).valid_value, None
        )
        self.assertEqual(
            SqlParameter(name='abc', value=Field(longValue=123)).valid_value, 123
        )
        self.assertEqual(
            SqlParameter(name='abc', value=Field(longValue=123)).valid_value, 123
        )
        self.assertEqual(SqlParameter(name='abc', value=Field()).valid_value, None)


class TestField(TestCase):
    def test_from_value(self) -> None:
        self.assertEqual(Field.from_value('str'), Field(stringValue='str'))
        self.assertEqual(Field.from_value(123), Field(longValue=123))
        self.assertEqual(Field.from_value(1.23), Field(doubleValue=1.23))
        self.assertEqual(Field.from_value(True), Field(booleanValue=True))
        self.assertEqual(Field.from_value(False), Field(booleanValue=False))
        self.assertEqual(Field.from_value(b'bytes'), Field(blobValue=b64encode(b'bytes')))
        self.assertEqual(Field.from_value(None), Field(isNull=True))

        class Dummy:
            pass

        with self.assertRaises(Exception):
            Field.from_value(Dummy())
