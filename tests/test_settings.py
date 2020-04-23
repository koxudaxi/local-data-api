from local_data_api.settings import setup


def test_setup_mysql(mocker) -> None:
    mock_register_secret = mocker.patch('local_data_api.settings.register_secret')
    mock_register_resource = mocker.patch('local_data_api.settings.register_resource')
    import os

    os.environ['ENGINE'] = 'MySQLJDBC'
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


def test_setup_postgres(mocker) -> None:
    mock_register_secret = mocker.patch('local_data_api.settings.register_secret')
    mock_register_resource = mocker.patch('local_data_api.settings.register_resource')
    import os

    os.environ['ENGINE'] = 'PostgreSQLJDBC'
    setup()
    mock_register_secret.assert_called_with(
        'postgres',
        'example',
        'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy',
    )
    mock_register_resource.assert_called_with(
        'arn:aws:rds:us-east-1:123456789012:cluster:dummy',
        'PostgreSQLJDBC',
        '127.0.0.1',
        5432,
        'postgres',
        'example',
        {'JAR_PATH': '/usr/lib/jvm/postgresql-java-client.jar'},
    )

def test_setup_postgres_no_jdbc(mocker) -> None:
    mock_register_secret = mocker.patch('local_data_api.settings.register_secret')
    mock_register_resource = mocker.patch('local_data_api.settings.register_resource')
    import os

    os.environ['ENGINE'] = 'PostgresSQL'
    setup()
    mock_register_secret.assert_called_with(
        'postgres',
        'example',
        'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy',
    )
    mock_register_resource.assert_called_with(
        'arn:aws:rds:us-east-1:123456789012:cluster:dummy',
        'PostgresSQL',
        '127.0.0.1',
        5432,
        'postgres',
        'example',
        {}
    )
