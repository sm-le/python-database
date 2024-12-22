# SQLite connector
# contributor: smlee

# History
# 2024-12-22 - v1.0.1 | add logging
# 2024-06-01 - v1.0.0 | refactored from old repo

# Module
import sqlite3
from typing import Union, List, Dict
import logging
logger = logging.getLogger('pydb')
from pydb.conf import log


# Main
class SQLiteConnector(object):
    """Instance method for SQLite3 connection and operation

    Args:
        _conn_medium (str): Connection medium
    """

    def __init__(self,
                 _conn_medium:str):
        self._conn = sqlite3.connect(_conn_medium)
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, 
                 exception_type, 
                 exception_value, 
                 traceback):
        self.conn_.close()
        self.cur_.close()

        if not exception_type:
            return True
        else:
            raise BaseException(f"Exit error: {exception_type}, {exception_value}, {traceback}")
    @log(set_logger=logger)
    def create_table(self,
                     *,
                     table_name:str, 
                     columns:Dict[str, str]):
        """Create table with given columns

        Args:
            table_name (str): Table name
            columns (Dict[str, str]): Column name and type
        """
        column_format = ','.join([f"[{k}] {v}" for k, v in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_format})"
        self._cursor.execute(query)
        self._conn.commit()

    @log(set_logger=logger)
    def insert(self,
               *,
               table_name:str,
               values:List[Dict[str, Union[str, int]]]):
        """Insert values into table

        Args:
            table_name (str): Table name
            values List(Dict[str, Union[str, int]]): Column name and value
        """
        assert len(values) > 0, "No values to insert"
        assert type(values) == list, "Values must be list"
        assert type(values[0]) == dict, "Elements must be dictionary"

        column_format = ','.join([f"[{k}]" for k in values[0].keys()])
        value_placeholder = ','.join(["?"]*len(values[0]))
        query = f"INSERT OR IGNORE INTO {table_name} " \
                f"({column_format}) VALUES ({value_placeholder})"
        self._cursor.executemany(query,[list(i.values()) for i in values])
        self._conn.commit()

    @log(set_logger=logger)
    def merge(self,
               *,
               table_name:str,
               values:List[Dict[str, Union[str, int]]]):
        """Insert values into table

        Args:
            table_name (str): Table name
            values List(Dict[str, Union[str, int]]): Column name and value
        """
        assert len(values) > 0, "No values to insert"
        assert type(values) == list, "Values must be list"
        assert type(values[0]) == dict, "Elements must be dictionary"

        column_format = ','.join([f"[{k}]" for k in values[0].keys()])
        value_placeholder = ','.join(["?"]*len(values[0]))
        query = f"INSERT OR REPLACE INTO {table_name} " \
                f"({column_format}) VALUES ({value_placeholder})"
        self._cursor.executemany(query,[list(i.values()) for i in values])
        self._conn.commit()

        # column_format = ','.join([f"[{k}]" for k in values.keys()])
        # value_format = ','.join([f"'{v}'" if type(v) == str else v for v in values.values()])
        # query = f"INSERT OR REPLACE INTO {table_name} " \
        #         f"({column_format}) VALUES ({value_format})"
        # self._cursor.execute(query)
        # self._conn.commit()
    @log(set_logger=logger)
    def select(self,
               *,
               table_name:str,
               columns:List[str],
               conditions:Dict[str, Union[str, int]]):
          """Select values from table
    
          Args:
                table_name (str): Table name
                columns (List[str]): Column names
                conditions (Dict[str, Union[str, int]]): Column name and value
          """
          column_format = ','.join([f"[{k}]" for k in columns])
          condition_format = ' AND '.join([f"[{k}] = '{v}'" if type(v) == str else f"[{k}] = {v}" for k, v in conditions.items()])
          query = f"SELECT {column_format} FROM {table_name} WHERE {condition_format}"
          self._cursor.execute(query)
          return self._cursor.fetchall()