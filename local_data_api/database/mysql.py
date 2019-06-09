from __future__ import annotations

from database.database import DataBase


class MySQL(DataBase):
    DIALECT = 'mysql'
    DRIVER = 'mysqldb'



