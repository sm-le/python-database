# Database connector for MariaDB/MySQL
# contributors: smlee

# History
# 2024-03-15 | v1.0 - refactored for a common tool

# Module import
import pymysql
from dataclasses import dataclass
from typing import Union
from conf.error_message import emsg

# Main
@dataclass
class mariaConnect(object):
    """Instance method for MariaDB connection and operation

    Args:
        conn_medium_: a connection medium either credential or connection pool
        pool_: a boolean for using connection pool, default: False
    """
    conn_medium_:Union[object,dict]
    pool_:object=False

    def __post_init__(self):
        try:
            if self.pool_:
                self.conn_:object=self.conn_medium_.connection()
                self.conn_.autocommit = True
                self.cur_:object=self.conn_.cursor(pymysql.cursors.DictCursor)
            else:
                self.conn_:object=pymysql.connect(**self.conn_medium_)
                self.conn_.autocommit = True
                self.cur_:object=self.conn_.cursor(pymysql.cursors.DictCursor)
        except pymysql.MySQLError as e:
            raise RuntimeError(f"Error connection to the database: {e}")

    def close(self):
        """Close the database connection
        """
        if self.conn_:
            self.conn_.close()
            self.conn_ = None
    
    def ping(self):
        """Reconnet to database if connection is lost
        """
        self.conn_.ping(reconnect=True)
        self.conn_.autocommit = True
        self.cur_:object=self.conn_.cursor(pymysql.cursors.DictCursor)

    def delete(self,
               query:str,
               database:str=None):
        """delete data from the database

        Args:
            query: a query string
            database: a target database
        """

        try:
            assert query
            assert query.lower().startswith("delete")
            if database:
                self.conn_.database = database
            
            try:
                self.cur_.execute(query)

                return True
            
            except pymysql.MySQLError as e:
                raise RuntimeError(f"Error while deleting data from the database: {e}")
            except Exception as e:
                raise RuntimeError(f"Error: {emsg(e)}")
        except:
            raise ValueError(f"Please set your query")
        
    def truncate(self,
                 table_name:str,
                 database:str):
        """truncate table within a database
        
        Args:
            table_name: a table name
            database: a target database with the table
        """
        try:
            assert table_name

            if database:
                table_name = f"{database}.{table_name}"
            
            try:
                query = f"TRUNCATE TABLE {table_name};"

                self.cur_.execute(query)

                return True
            
            except pymysql.MySQLError as e:
                raise RuntimeError(f"Error while deleting data from the database: {e}")
            except Exception as e:
                raise RuntimeError(f"Error: {emsg(e)}")
        except:
            raise ValueError(f"Please set your table name")

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
        try:
            assert query
            assert query.lower().startswith("select")
            # check database name
            if database:
                self.conn_.database = database
            
            try:
                # check connection
                if not self.conn_:
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
                raise RuntimeError(f"Error: {emsg(e)}")
        except:
            raise ValueError(f"Please set your query")
        
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

        try:
            # check data type
            assert type(data) == list
            assert len(data) > 0

            try:
                fields = data[0].keys()
                assert all(row.keys() == fields for row in data[1:])
                
                # check database connection
                if not self.conn_:
                    self.ping()
                # add database name if exists
                if database:
                    self.conn_.database = database
                try:
                    # make data formats
                    fields_format = ", ".join(fields)
                    values_format = ", ".join([f'({", ".join([f"%({i})" for i in fields])})'])

                    # make insert ignore query
                    query = f"INSERT IGNORE INTO {table_name} " \
                            f"({fields_format}) " \
                            f"VALUES {values_format}"
                    # insert data
                    self.cur_.executemany(query,
                                          data)
                    
                except pymysql.MySQLError as e:
                    raise RuntimeError(f"Error while inserting data from the database: {e}")
                except Exception as e:
                    raise RuntimeError(f"Error: {emsg(e)}")

            except:
                raise TypeError(f"All fields must be identical.")

        except:
            raise ValueError(f"Input data != type(list) or empty")
    
    def merge(self,
              *,
              data:list,
              table_name:str,
              database:str=None,
              update_targets=None):
        """Merge data into the database

        Args:
            data: data(s) to insert
            table_name: a table name
            database: a database name
            update_targets: a list of features to update if the record already exists
        """

        try:
            # check data type
            assert type(data) == list
            assert len(data) > 0

            try:
                fields = data[0].keys()
                assert all(row.keys() == fields for row in data[1:])
                
                # check database connection
                if not self.conn_:
                    self.ping()
                # add database name if exists
                if database:
                    self.conn_.database = database
                try:
                    # make data formats
                    fields_format = ", ".join(fields)
                    values_format = ", ".join([f'({", ".join([f"%({i})" for i in fields])})'])

                    # get features name to merge, if it is None it will update entire features given in fields
                    if update_targets:
                        field_names = [i for i in fields if i in update_targets]
                        update_format = ", ".join(f"{i}=VALUES({i})" for i in field_names)
                    else:
                        update_format = ", ".join(f"{i}=VALUES({i})" for i in fields)

                    # make insert ignore query
                    query = f"INSERT IGNORE INTO {table_name} " \
                            f"({fields_format}) " \
                            f"VALUES {values_format} " \
                            f"ON DUPLICATE KEY UPDATE " \
                            f"{update_format};"
                    
                    # insert data
                    self.cur_.executemany(query,
                                          data)
                    
                except pymysql.MySQLError as e:
                    raise RuntimeError(f"Error while inserting data from the database: {e}")
                except Exception as e:
                    raise RuntimeError(f"Error: {emsg(e)}")

            except:
                raise TypeError(f"All fields must be identical.")

        except:
            raise ValueError(f"Input data != type(list) or empty")


