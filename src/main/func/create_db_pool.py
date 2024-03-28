# pooled DB object
# contributor: smlee

# History
# 2024-03-27 | v1.0 - first commit

# Python module
import json
from dbutils.pooled_db import PooledDB
import pymysql
from conf.error_message import emsg

# Main
def pooledDB(dbargs:dict) -> object:
    """Make PooledDB object with DBUtils

    Args:
        dbargs: database info
    Returns:
        object(pooled_DB)
    """
    try:
        ## Setup
        if "database" in dbargs:
            db_credential = {"host" : dbargs["host"],
                            "user" : dbargs["user"],
                            "password" : dbargs["password"],
                            "port" : dbargs["port"],
                            "database":dbargs["database"],
                            "autocommit": True}
        else:
            db_credential = {"host" : dbargs["host"],
                            "user" : dbargs["user"],
                            "password" : dbargs["password"],
                            "port" : dbargs["port"],
                            "autocommit": True}
            
        ## Pool
        pool_setting = {"creator":pymysql,
                        "maxconnections":5,
                        "ping":1,
                        **db_credential}

        ## PooledDB
        pooled_DB = PooledDB(**pool_setting)

        return pooled_DB
    
    except Exception as e:
        print(f"Error while creating DB pool: {emsg(e)}")