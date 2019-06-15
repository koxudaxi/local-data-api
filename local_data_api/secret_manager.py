from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict
from hashlib import sha1

from local_data_api.exceptions import BadRequestException


@dataclass
class Secret:
    user_name: Optional[str] = None
    password: Optional[str] = None


SECRETS: Dict[str, Secret] = {}


def create_secret_arn(region_name: str = 'us-east-1', account: str = '123456789012') -> str:
    return f'arn:aws:secretsmanager:{region_name}:{account}:secret:local-data-api{sha1().hexdigest()}'


def register_secret(user_name: Optional[str], password: Optional[str], secret_arn: Optional[str]) -> str:
    if not secret_arn:
        secret_arn = create_secret_arn()

    SECRETS[secret_arn] = Secret(user_name, password)

    return secret_arn


def get_secret(secret_arn: str) -> Secret:
    if secret_arn in SECRETS:
        return SECRETS[secret_arn]
    raise BadRequestException(f'Error fetching secret {secret_arn} : Secrets Manager can’t find the specified '
                              f'secret. (Service: AWSSecretsManager; Status Code: 400; Error Code: '
                              f'ResourceNotFoundException; Request ID:  00000000-1111-2222-3333-44444444444)')
