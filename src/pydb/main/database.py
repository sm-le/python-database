# Database Connector Main for all database connections
# contributor: smlee

# History
# 2024-03-28 | v1.1 - add insert method
# 2024-03-27 | v1.0 - first commit

# Module import
from util.mysql.main import mariaConnect
from util.mongo.main import mongoConnect
from util.azure.main import AzureTable
from .func.create_db_pool import pooledDB
from .func.get_secrets import get_secret
from typing import List, Union, Dict
from dataclasses import dataclass

# Main
@dataclass
class databaseConnect:
    """Database connection class
    
    Args:
        database: database name
        * current database list:
            - signal
            - record
            - sequence
            - azure
        override: optional argument
    """
    name:str
    override:bool=False

    def __post_init__(self):
        secret = get_secret(self.name,self.override)

        if self.name == "signal" or self.name == "record":
            self.database = mariaConnect(pooledDB(secret),True)

        elif self.name == "sequence":
            self.database = mongoConnect(secret)
            
        elif self.name == "azure":
            self.database = AzureTable(secret)
            
        else:
            raise TypeError(f"Invalid database: {self.name}")
        
    def __enter__(self):
        """Instantiate mariaConnect class object"""

        return self
    
    def __exit__(self,
                 exception_type,
                 exception_value,
                 traceback):
        """Exit instantiation from __enter__
        """

        self.database.close()

        if not exception_type:
            return True
        else:
            raise BaseException(f"Exit error: {exception_type}, {exception_value}, {traceback}")
            
    def select(self,
               *,
               query:str=None,
               database:str=None,
               features:list=None,
               parameters:dict=None,
               name_filter:str=None,
               collection_name:str=None) -> List:
        """Execute SELECT command from MariaDB or findall from MongoDB or query entity from Azure Table Storage 
        using either input query (MariaDB) or conditions (MongoDB) or entity (Azure Table Storage) 

        Args:
            For MariaDB,
                query: a query for MariaDB
                database (optional): database name for MariaDB // table_name for Azure Table
            For MongoDB,
                query: a query for MongoDB
                collection_name: a collection name for MongoDB
                database (optional): a database name for MongoDB
            For AzureTable,
                features: a list of columns to select from Azure Table (named select)
                parameters: a dictionary of parameters for Azure Table
                name_filter: a filter for Azure Table
                database: a table name for Azure Table
        Returns:
            list(rows matched conditions)
        """

        try:
            if self.name == "mariadb":
                assert query
                return self.database.select(query,database)
            elif self.name == "mongodb":
                assert query
                assert collection_name
                return self.database.find(query,collection_name)
            elif self.name == "azure":
                assert features
                assert parameters
                assert name_filter
                return self.database.query_entity(select=features,
                                                 parameters=parameters,
                                                 name_filter=name_filter,
                                                 table_name=database)
        except:
            raise ValueError("Invalid input arguments")

    def insert(self,
               *,
               data:Union[List,Dict]=None,
               table_name:str=None,
               collection_name:str=None,
               database:str=None,
               is_merge_mode:bool=False):
        """Insert data into the database
        
        Args:
        
        
        """
        try:
            if self.name == "mariadb":
                    assert data
                    assert table_name
                    if is_merge_mode:
                        self.database.merge(data=data,
                                            table_name=table_name,
                                            database=database)
                    else:
                        self.database.insert(data=data,
                                             table_name=table_name,
                                             database=database)
            elif self.name == "mongodb":
                assert data
                assert collection_name
                if database:
                    self.database.insert(rows=data,
                                         collection_name=collection_name,
                                         database=database,
                                         is_merge_mode=is_merge_mode)
                else:
                    self.database.insert(rows=data,
                                         collection_name=collection_name,
                                         is_merge_mode=is_merge_mode)
            elif self.name == "azure":
                assert data
                assert database
                return self.database.insert_entity(entity=data,
                                                   table_name=database)
        except:
            raise ValueError("Invalid input arguments")
        
    def close(self):
        """Close database connection"""
        self.database.close()