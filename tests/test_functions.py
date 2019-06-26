
from base64 import b64encode
from unittest import TestCase

from local_data_api import convert_value


class TestLocalDataAPIFunction(TestCase):
    def setUp(self) -> None:
        pass

    def test_convert_value(self) -> None:
        self.assertDictEqual(convert_value('str'), {'stringValue': 'str'})
        self.assertDictEqual(convert_value(123), {'longValue': 123})
        self.assertDictEqual(convert_value(1.23), {'doubleValue': 1.23})
        self.assertDictEqual(convert_value(True), {'booleanValue': True})
        self.assertDictEqual(convert_value(False), {'booleanValue': False})
        self.assertDictEqual(convert_value(b'bytes'), {'blobValue': b64encode(b'bytes')})
        self.assertDictEqual(convert_value(None), {'isNull': True})

        class Dummy:
            pass
        with self.assertRaises(Exception):
            convert_value(Dummy())

