from __future__ import annotations

from local_data_api.resources.resource import Resource, register_resource


@register_resource
class SQLite(Resource):
    DIALECT = 'sqlite'
    DRIVER = None
