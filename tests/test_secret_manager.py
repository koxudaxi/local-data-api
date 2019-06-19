
from unittest import TestCase

from local_data_api.exceptions import BadRequestException
from local_data_api.secret_manager import register_secret, get_secret, create_secret_arn, SECRETS, Secret


class TestSecretManagerFunction(TestCase):
    def setUp(self) -> None:
        SECRETS.clear()

    def test_create_secret_arn(self) -> None:
        self.assertEqual(create_secret_arn()[:67], 'arn:aws:secretsmanager:us-east-1:123456789012:secret:local-data-api')
        self.assertEqual(create_secret_arn(region_name='ap-northeast-1', account='000000000000')[:72],
                         'arn:aws:secretsmanager:ap-northeast-1:000000000000:secret:local-data-api')

    def test_register_secret(self) -> None:

        register_secret(user_name='root', password='example',
                        secret_arn='arn:aws:secretsmanager:ap-northeast-1:000000000000:secret:local-data-api')

        secret: Secret = SECRETS['arn:aws:secretsmanager:ap-northeast-1:000000000000:secret:local-data-api']
        self.assertEqual(secret, Secret('root', 'example'))

    def test_register_secret_no_arn(self) -> None:

        secret_arn: str = register_secret('root', 'example')

        secret: Secret = SECRETS[secret_arn]
        self.assertEqual(secret, Secret('root', 'example'))

    def test_get_secret(self) -> None:
        secret_arn = 'arn:aws:secretsmanager:ap-northeast-1:000000000000:secret:local-data-api'

        expected_secret: Secret = Secret('root', 'example')
        SECRETS[secret_arn] = expected_secret

        secret: Secret = get_secret(secret_arn)

        self.assertEqual(secret, expected_secret)

    def test_get_secret_notfound_secret(self) -> None:
        secret_arn = 'arn:aws:secretsmanager:ap-northeast-1:000000000000:secret:local-data-api'

        exception = BadRequestException(f'Error fetching secret {secret_arn} : Secrets Manager can’t find the specified '
                            f'secret. (Service: AWSSecretsManager; Status Code: 400; Error Code: '
                            f'ResourceNotFoundException; Request ID:  00000000-1111-2222-3333-44444444444)')

        with self.assertRaises(BadRequestException, ):
            try:
                get_secret(secret_arn)
            except BadRequestException as e:
                self.assertEqual(e.message, exception.message)
                raise
