from __future__ import annotations

import os

from pydantic import BaseModel

from local_data_api.resources.resource import register_resource
from local_data_api.secret_manager import register_secret

RESOURCE_ARN: str = os.environ.get(
    'RESOURCE_ARN', 'arn:aws:rds:us-east-1:123456789012:cluster:dummy'
)
SECRET_ARN: str = os.environ.get(
    'SECRET_ARN', 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy'
)


class DBSetting(BaseModel):
    HOST: str
    PORT: int
    USER: str
    PASSWORD: str
    JAR_PATH: str


def setup() -> None:
    # TODO: implement to create custom resource
    engine: str = os.environ.get('ENGINE', 'MySQLJDBC')
    if engine.upper().startswith('MYSQL'):
        db_setting: DBSetting = DBSetting(
            HOST=os.environ.get('MYSQL_HOST', '127.0.0.1'),
            PORT=os.environ.get('MYSQL_PORT', '3306'),
            USER=os.environ.get('MYSQL_USER', 'root'),
            PASSWORD=os.environ.get('MYSQL_PASSWORD', 'example'),
            JAR_PATH=os.environ.get(
                'MYSQL_JDBC_JAR_PATH', '/usr/lib/jvm/mariadb-java-client.jar'
            ),
        )
    else:
        db_setting = DBSetting(
            HOST=os.environ.get('POSTGRES_HOST', '127.0.0.1'),
            PORT=os.environ.get('POSTGRES_PORT', '5432'),
            USER=os.environ.get('POSTGRES_USER', 'postgres'),
            PASSWORD=os.environ.get('POSTGRES_PASSWORD', 'example'),
            JAR_PATH=os.environ.get(
                'POSTGRES_JDBC_JAR_PATH', '/usr/lib/jvm/postgresql-java-client.jar'
            ),
        )

    register_secret(db_setting.USER, db_setting.PASSWORD, SECRET_ARN)
    register_resource(
        RESOURCE_ARN,
        engine,
        db_setting.HOST,
        db_setting.PORT,
        db_setting.USER,
        db_setting.PASSWORD,
        {'JAR_PATH': db_setting.JAR_PATH} if 'JDBC' in engine.upper() else {},
    )
