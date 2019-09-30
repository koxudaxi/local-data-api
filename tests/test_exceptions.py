from local_data_api.exceptions import DataAPIException


def test_code():
    class DummyError(DataAPIException):
        STATUS_CODE = 500

    assert DummyError('test').code == 'DummyError'
