# Database connector for MariaDB/MySQL
# contributors: smlee

# History
# 2024-12-22 | v1.3 - remove dataclass
# 2024-06-02 | v1.2 - add transaction check and logger
# 2024-03-18 | v1.1 - add entrypoint, fixed ping method
# 2024-03-15 | v1.0 - refactored for a common tool

# Module import
import pymysql
from typing import Union, List, Dict
import logging
logger = logging.getLogger('pydb')
from pydb.conf import log

# Main
class mariaConnect(object):
    """Instance method for MariaDB connection and operation
    """
    def __init__(self,
                 conn_medium_:object|dict,
                 pool_:bool=False):
        """Instantiate

        Args:
            conn_medium_ (object|dict): a connection medium either credential or connection pool
            pool_ (bool): a boolean for using connection pool, default: False
        """
        try:
            self.conn_medium_ = conn_medium_
            self.pool_ = pool_
            self.conn_:object=self.conn_medium_.connection() if self.pool_ else pymysql.connect(**self.conn_medium_)
            self.cur_:object=self.conn_.cursor(pymysql.cursors.DictCursor)
        except pymysql.MySQLError as e:
            raise RuntimeError(f"Error connection to the database: {e}")

    def __enter__(self):
        """Instantiate mariaConnect class object"""

        return self
    
    def __exit__(self,
                 exception_type,
                 exception_value,
                 traceback):
        """Exit instantiation from __enter__
        """
        self.conn_.close()
        self.cur_.close()

        if not exception_type:
            return True
        else:
            raise BaseException(f"Exit error: {exception_type}, {exception_value}, {traceback}")

    def close(self):
        """Close the database connection
        """
        if self.conn_:
            self.conn_.close()
            self.cur_.close()
            self.conn_medium_.close()
            self.conn_ = None
    
    @log(set_logger=logger)
    def ping(self):
        """Reconnet to database if connection is lost
        """
        self.conn_=self.conn_medium_.connection() if self.pool_ else pymysql.connect(**self.conn_medium_)
        self.cur_:object=self.conn_.cursor(pymysql.cursors.DictCursor)
    @log(set_logger=logger)
    def delete(self,
               query:str,
               database:str=None):
        """delete data from the database

        Args:
            query: a query string
            database: a target database
        """

        assert query, "Please set your query"
        assert query.lower().startswith("delete"), "Query must start with DELETE"
        assert "--" not in query.lower(), "Please remove comment in the query"
        if database:
            self.conn_.database = database
        
        try:
            self.cur_.execute(query)
            if self.cur_.rowcount > 0:
                self.conn_.commit()
                return True
            else:
                self.conn_.rollback()
                raise RuntimeError("No data deleted")
        
        except pymysql.MySQLError as e:
            raise RuntimeError(f"Error while deleting data from the database: {e}")
        except Exception as e:
            raise RuntimeError(f"{e}")

    @log(set_logger=logger) 
    def truncate(self,
                 table_name:str,
                 database:str):
        """truncate table within a database
        
        Args:
            table_name: a table name
            database: a target database with the table
        """
        try:
            assert table_name, "Please set your table name"

            if database:
                table_name = f"{database}.{table_name}"
            
            try:
                query = f"TRUNCATE TABLE {table_name};"

                self.cur_.execute(query)
                self.conn_.commit()

                return True
            
            except pymysql.MySQLError as e:
                raise RuntimeError(f"Error while deleting data from the database: {e}")
            except Exception as e:
                raise RuntimeError(f"{e}")
        except:
            raise ValueError(f"Please set your table name")
    @log(set_logger=logger)
    def select(self,
               query:str,
               database:str=None,
               chunk_size:int=1000000):
        """select data from the database

        Args:
            query: a query string
                    e.g) SELECT * FROM table
            database: a target database
            chunk_size: how to split data size on large select
        Returns:
            list(dict(rst))
        """
        
        # check query 
        
        assert query, "Please set your query"
        assert query.lower().startswith("select"), "Query must start with SELECT"
        assert "--" not in query.lower(), "Please remove comment in the query"

        # check database name
        if database:
            self.conn_.database = database
        
        try:
            # check connection
            if (not self.pool_ and not self.conn_.open) or not self.conn_:
                self.ping()
            # execute query
            self.cur_.execute(query)
            # fetch data
            result = list()
            complete = False
            while not complete:
                rst = self.cur_.fetchmany(size=chunk_size)

                for i in rst:
                    result.append(i)
                
                complete = len(rst) < chunk_size
            
            return result
        
        except pymysql.MySQLError as e:
            raise RuntimeError(f"Error while fetching data from the database: {e}")
        except Exception as e:
            raise RuntimeError(f"{e}")
        
    @log(set_logger=logger)   
    def insert(self,
               *,
               data:list,
               table_name:str,
               database:str=None):
        """Insert data to the database

        Args:
            data: data(s) to insert
            table_name: a table name
            database: a database name
        """

        
        # check data type
        assert type(data) == list, "Data type must be a list"
        assert len(data) > 0, "Data is empty"

        fields = data[0].keys()
        assert all(row.keys() == fields for row in data[1:]), "All fields must be identical"
        
        # check database connection
        if (not self.pool_ and not self.conn_.open) or not self.conn_:
            self.ping()
        # add database name if exists
        if database:
            self.conn_.database = database
        try:
            # make data formats
            fields_format = ", ".join(fields)
            values_format = ", ".join([f'({", ".join([f"%({i})s" for i in fields])})'])

            # make insert ignore query
            query = f"INSERT IGNORE INTO {table_name} " \
                    f"({fields_format}) " \
                    f"VALUES {values_format}"
            # insert data
            self.cur_.executemany(query,
                                    data)

            if self.cur_.rowcount > 0:
                self.conn_.commit()
            else:
                self.conn_.rollback()
                raise RuntimeError("No data inserted")
            
        except pymysql.MySQLError as e:
            raise RuntimeError(f"Error while inserting data from the database: {e}")
        except Exception as e:
            raise RuntimeError(f"{e}")

    @log(set_logger=logger)
    def merge(self,
              *,
              data:List[Dict],
              table_name:str,
              database:str=None,
              update_targets:Union[List,str]=None,
              increment:bool=False):
        """Merge data into the database

        Args:
            data: data(s) to insert
            table_name: a table name
            database: a database name
            update_targets: a list of features to update if the record already exists
            increment: a boolean for single count increment update
        """


        # check data type
        assert type(data) == list, "Data type must be a list"
        assert len(data) > 0, "Data is empty"


        fields = data[0].keys()
        assert all(row.keys() == fields for row in data[1:]), "All fields must be identical"
        
        # check database connection
        if (not self.pool_ and not self.conn_.open) or not self.conn_:
            self.ping()
        # add database name if exists
        if database:
            self.conn_.database = database
        try:
            # make data formats
            fields_format = ", ".join(fields)
            values_format = ", ".join([f'({", ".join([f"%({i})s" for i in fields])})'])
    
            if increment:
                assert type(update_targets) == str, "update_targets must be a string"
                query = f"INSERT IGNORE INTO {table_name} " \
                        f"({fields_format}) " \
                        f"VALUES {values_format} " \
                        f"ON DUPLICATE KEY UPDATE " \
                        f"{update_targets}={update_targets}+1;"
            else:
                # get features name to merge, if it is None it will update entire features given in fields
                if update_targets:
                    assert type(update_targets) == list or type(update_targets) == str, "update_targets must be a list or str"
                    if type(update_targets) == str:
                        field_names = [i for i in fields if i == update_targets]
                    else:
                        field_names = [i for i in fields if i in update_targets]
                    update_format = ", ".join(f"{i}=VALUES({i})" for i in field_names)
                else:
                    update_format = ", ".join([f"{i}=VALUES({i})" for i in fields])
                # make insert ignore query
                query = f"INSERT IGNORE INTO {table_name} " \
                        f"({fields_format}) " \
                        f"VALUES {values_format} " \
                        f"ON DUPLICATE KEY UPDATE " \
                        f"{update_format};"
            
            # insert data
            self.cur_.executemany(query,
                                  data)
            
            if self.cur_.rowcount > 0:
                self.conn_.commit()
            else:
                self.conn_.rollback()
                raise RuntimeError("No data inserted")
            
        except pymysql.MySQLError as e:
            raise RuntimeError(f"Error while inserting data from the database: {e}")
        except Exception as e:
            raise RuntimeError(f"{e}")

