# Database connector for MongoDB
# contributor: smlee

# History
# 2024-12-22 | v1.1 - removed dataclass
# 2024-03-15 | v1.0 - refactored for a common tool

# Module import
import pymongo
from typing import Dict

# Main
class mongoConnect(object):
    """Instance method for MongoDB connection and operation
    """
    
    def __init__(self,
                 conn_medium_:Dict[str,int|str]):
        """Instantiate

        Args:
            conn_medium_: a connection medium (e.g. credential)
        """
        try:
            self.conn_medium_ = conn_medium_
            self.conn_ = pymongo.MongoClient(**self.conn_medium_)

        except pymongo.errors.PyMongoError as e:
            raise RuntimeError(f"Error connection to the database: {e}")

    def __enter__(self):
        """Instantiate mongoConnect class object"""

        return self
    
    def __exit__(self,
                 exception_type,
                 exception_value,
                 traceback):
        """Exit instantiation from __enter__
        """

        self.conn_.close()

        if not exception_type:
            return True
        else:
            raise BaseException(f"Exit error: {exception_type}, {exception_value}, {traceback}")

    def close(self):
        """Close the database connection
        """
        if self.conn_:
            self.conn_.close()
    
    def ping(self):
        """Reconnect to database if conneciton is lost
        """

        try:
            self.conn_.admin.command('ping')
        except pymongo.errors.ConnectionFailure:
            self.conn_ = pymongo.MongoClient(**self.conn_medium_)
        except pymongo.errors.OperationFailure as e:
            raise RuntimeError(f"Error encountered: {e}")
    
    def check_database(self,
                       database:str):
        """Check database availability

        Args:
            database: a database name
        """

        databases = set(self.conn_.list_database_names())
        assert database in databases, f"Please check your database name"

    def check_collection(self,
                         collection_name:str,
                         is_create_mode:bool=False):
        """Check collection availability

        Args:
            collection_name: a collection name
            is_create_mode: creating new collection?
        """

        if not is_create_mode:
            collections = set(self.db_.list_collection_names())
            assert collection_name in collections, "Please check your collection name"
        

    def find(self,
             query:dict,
             collection_name:str,
             database:str="ift_sequence") -> list:
        """Execute find command using input query and fetch 
        data from collection_name within a database

        Args:
            query: a MongoDB command for find
            collection_name: a collection name
            database: a database name
        """

    
        assert query, "Please set your query"
        # ping database
        self.ping()

        # set database to mongoclient
        if database:
            self.check_database(database)
            self.db_ = self.conn_[database]

        try:
            self.check_collection(collection_name)
            self.col_ = self.db_[collection_name]

            handle = self.col_.find(query)

            # fetch result
            result = list()
            for row in handle:
                result.append(row)

            return result
        
        except pymongo.errors.OperationFailure as e:
            raise RuntimeError(f"Error while fetching data from the database: {e}")
        except pymongo.errors.PyMongoError as e:
            raise RuntimeError(f"{e}")
    
    def insert(self,
               *
               rows:list,
               collection_name:str,
               database:str="ift_sequence",
               is_merge_mode:bool=False):
        """Insert documents to a collection given by collection name

        Args:
            rows: a list of documents
            collection_name: a collection name
            database: a database name
            is_merge_mode: whether to upsert data on duplicate record
        """

        
        assert type(rows) == list, "Please set your data as list"
        assert len(rows) > 0, "Please set your data"

        # ping database
        self.ping()

        # set database to mongoclient
        if database:
            self.check_database(database)
            self.db_ = self.conn_[database]

        try:
            self.check_collection(collection_name)
            self.col_ = self.db_[collection_name]

            # insert data
            self.col_.insert_many(rows)
        
        except pymongo.errors.BulkWriteError:
            if is_merge_mode:
                # collect record to find based on primary key
                duplicate_records = set()
                for search_record in self.col_.find({'_id':{'$in':[row['_id'] for row in rows]}}):
                    duplicate_records.add(search_record['_id'])
                
                # collect document to write and classify document into replace and insert
                records_to_write = list()
                for row in rows:
                    if row['_id'] in duplicate_records:
                        records_to_write.append(pymongo.ReplaceOne( {"_id":row["_id"]},
                                                                    row,
                                                                    upsert=True ))
                    else:
                        records_to_write.append(pymongo.InsertOne( row ))
                
                # initiate bulk write
                self.col_.bulk_write(records_to_write)
            else:
                raise RuntimeError(f"Duplicate record found and merge mode is not enabled: {e}")
        except pymongo.errors.OperationFailure as e:
            raise RuntimeError(f"Error while inserting data from the database: {e}")
        except pymongo.errors.PyMongoError as e:
            raise RuntimeError(f"Error: {e}")
        
    def delete(self,
               *
               query:dict,
               collection_name:str,
               database:str,
               override:bool=False):
        """Delete documents from a collection given by collection name

        Args:
            rows: a list of documents
            collection_name: a collection name
            database: a database name
            override: a layer of safety to prevent accident
        """

        
        assert query, "Please set your query"
        # ping database
        self.ping()

        # set database to mongoclient
        if database:
            self.check_database(database)
            self.db_ = self.conn_[database]

        try:
            self.check_collection(collection_name)
            self.col_ = self.db_[collection_name]

            # delete data
            if override:
                self.col_.delete_many(query)
        
        except pymongo.errors.OperationFailure as e:
            raise RuntimeError(f"Error while deleting data from the database: {e}")
        except pymongo.errors.PyMongoError as e:
            raise RuntimeError(f"Error: {e}")