from local_data_api.settings import setup


def test_setup(mocker) -> None:
    mock_register_secret = mocker.patch('local_data_api.settings.register_secret')
    mock_register_resource = mocker.patch('local_data_api.settings.register_resource')
    setup()
    mock_register_secret.assert_called_with(
        'root', 'example', 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy'
    )
    mock_register_resource.assert_called_with(
        'arn:aws:rds:us-east-1:123456789012:cluster:dummy',
        'MySQLJDBC',
        '127.0.0.1',
        3306,
        'root',
        'example',
        {'JAR_PATH': '/usr/lib/jvm/mariadb-java-client.jar'},
    )
