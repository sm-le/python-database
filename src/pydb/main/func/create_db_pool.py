# pooled DB object
# contributor: smlee

# History
# 2024-06-02 | v2.0 - refactored and added Singleton pattern
# 2024-03-27 | v1.0 - first commit

# Python module
import json
from dbutils.pooled_db import PooledDB
import pymysql
from dataclasses import dataclass

# Main
class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


@dataclass
class DBPool(metaclass=Singleton):
    """Make PooledDB object with DBUtils

    Args:
        dbargs: database info
    Returns:
        object(pooled_DB)
    """
    _dbc:dict

    def __post_init__(self):
        self._pool_config = {"creator":pymysql,
                             "mincached":1,
                             "maxcached":5,
                             "maxshared":3,
                             "maxconnections":10,
                             **self._dbc}
    
    def get_pool(self):
        try:
            return PooledDB(**self._pool_config)
        except Exception as e:
            raise ValueError(f"Error: {e}")