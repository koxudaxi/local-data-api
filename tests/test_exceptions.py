
from unittest import TestCase

from local_data_api.exceptions import DataAPIException


class TestDataAPIException(TestCase):
    def test_code(self) -> None:
        class DummyError(DataAPIException):
            STATUS_CODE = 500
        self.assertEqual(DummyError('test').code, 'DummyError')
