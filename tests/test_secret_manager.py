import pytest

from local_data_api.exceptions import BadRequestException
from local_data_api.secret_manager import (
    SECRETS,
    Secret,
    create_secret_arn,
    get_secret,
    register_secret,
)


@pytest.fixture()
def secrets():
    SECRETS.clear()
    return


def test_create_secret_arn(secrets) -> None:
    assert (
        create_secret_arn()[:67]
        == 'arn:aws:secretsmanager:us-east-1:123456789012:secret:local-data-api'
    )
    assert (
        create_secret_arn(region_name='ap-northeast-1', account='000000000000')[:72]
        == 'arn:aws:secretsmanager:ap-northeast-1:000000000000:secret:local-data-api'
    )


def test_register_secret(secrets) -> None:
    register_secret(
        user_name='root',
        password='example',
        secret_arn='arn:aws:secretsmanager:ap-northeast-1:000000000000:secret:local-data-api',
    )

    secret: Secret = SECRETS[
        'arn:aws:secretsmanager:ap-northeast-1:000000000000:secret:local-data-api'
    ]
    assert secret == Secret('root', 'example')


def test_register_secret_no_arn(secrets) -> None:
    secret_arn: str = register_secret('root', 'example')

    secret: Secret = SECRETS[secret_arn]
    assert secret == Secret('root', 'example')


def test_get_secret(secrets) -> None:
    secret_arn = (
        'arn:aws:secretsmanager:ap-northeast-1:000000000000:secret:local-data-api'
    )

    expected_secret: Secret = Secret('root', 'example')
    SECRETS[secret_arn] = expected_secret

    secret: Secret = get_secret(secret_arn)

    assert secret == expected_secret


def test_get_secret_notfound_secret(secrets) -> None:
    secret_arn = (
        'arn:aws:secretsmanager:ap-northeast-1:000000000000:secret:local-data-api'
    )

    message = (
        f'Error fetching secret {secret_arn} : Secrets Manager can’t find the specified '
        f'secret. (Service: AWSSecretsManager; Status Code: 400; Error Code: '
        f'ResourceNotFoundException; Request ID:  00000000-1111-2222-3333-44444444444)'
    )

    with pytest.raises(BadRequestException) as e:
        get_secret(secret_arn)
    assert message == str(e.value)
