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
