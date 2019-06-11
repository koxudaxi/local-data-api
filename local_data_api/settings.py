from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from local_data_api.resources.resource import create_resource
from local_data_api.secret_manager import add_secret


@dataclass
class Setting:
    engine: str
    host: Optional[str] = None
    port: Optional[int] = None


RESOURCE_ARN: str = os.environ.get('RESOURCE_ARN', 'dummy')
SECRET_ARN: str = os.environ.get('SECRET_ARN', 'dummy')

MYSQL_HOST: str = os.environ.get('MYSQL_HOST', '127.0.0.1')
ENGINE: str = os.environ.get('ENGINE', 'MySQL')
MYSQL_PORT: int = int(os.environ.get('MYSQL_PORT', '3306'))

MYSQL_USER: str = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD: str = os.environ.get('MYSQL_PASSWORD', 'example')


def setup():
    # TODO: implement to create custom resource
    add_secret(MYSQL_USER, MYSQL_PASSWORD, SECRET_ARN)
    create_resource(ENGINE, MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, RESOURCE_ARN)
