from __future__ import annotations

import os
from typing import Dict, Union


DATABASE_SETTINGS: Dict[str, Dict[str, Union[str, int]]] = {
    'MySQL': {
        'host': os.environ.get('MYSQL_HOST', '127.0.0.1'),
        'port': int(os.environ.get('MYSQL_PORT', '3306')),
        'user_name': os.environ.get('MYSQL_USER', 'root'),
        'password': os.environ.get('MYSQL_PASSWORD', 'example')
         }
}

