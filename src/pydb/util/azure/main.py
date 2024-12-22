# Azure storage table connector
# contributors: smlee

# History
# 2024-12-22 | v1.1 - removed dataclass
# 2024-03-23 | v1.0 - moved and refactored

# Module import
from azure.data.tables.aio import TableClient
from azure.core.exceptions import ResourceExistsError, HttpResponseError
from azure.data.tables import UpdateMode, TableTransactionError, TableEntity, TransactionOperation
from typing import Any, List, Mapping, Tuple, Union, Dict

# Set Types
EntityType = Union[TableEntity, Mapping[str, Any]]
OperationType = Union[TransactionOperation, str]
TransactionOperationType = Union[Tuple[OperationType, EntityType], Tuple[OperationType, EntityType, Mapping[str, Any]]]

# Main
class AzureTable:
    """Azure Table class to interact with Azure Table Storage
    """
    def __init__(self,conn_medium_:Dict[str,str|int]):
        """Instatiate

        Args:
            conn_medium_: a connection medium (e.g. credential)
        """
        try:
            self.conn_medium_ = conn_medium_
            storage_name = self.conn_medium_.get('storage name')
            account_key = self.conn_medium_.get('account key')
            self.connection_string = f"DefaultEndpointsProtocol=https;" \
                                     f"AccountName={storage_name};" \
                                     f"AccountKey={account_key};" \
                                     f"EndpointSuffix=core.windows.net"
        except HttpResponseError as e:
            raise RuntimeError(f"Error connection to the database: {e}")
        except Exception as e:
            raise RuntimeError(f"{e}")
    
    def _format_batch_operation(self,
                                *,
                                entities:list,
                                ctype:str) -> list:
        """Format a given list to batch operation accepted format

        Args:
            entities: a list of entity
            ctype: command type
                    e.g.) create, delete, upsert, update
        """

        entities = [(ctype,i,{'mode':'merge'}) if ctype == 'upsert' or ctype == 'update' \
                    else (ctype,i) for i in entities]

        return entities

    async def create_table(self,
                           table_name:str) -> bool:
        """Create a table within a storage account
        
        Args:
            table_name: a table name to create
            
        Returns:
            bool
        """
        async with TableClient.from_connection_string(conn_str=self.connection_string,
                                                      table_name=table_name) as tClient:
            try:
                _ = await tClient.create_table()
                return True
            except ResourceExistsError as e:
                raise RuntimeError(f"Table already exists: {e}")
            
    async def delete_table(self,
                           *,
                           table_name:str) -> bool:
        """Delete a table within a storage account
        
        Args:
            table_name: a table name to delete
        Returns:    
            bool
        """

        async with TableClient.from_connection_string(conn_str=self.connection_string,
                                                      table_name=table_name) as tClient:
            try:
                _ = await tClient.delete_table()
                return True
            except HttpResponseError as e:
                raise RuntimeError(f"Error while deleting table: {e}")
            
    async def insert_entity(self,
                            *,
                            entity:EntityType,
                            table_name:str) -> bool:
        """Insert entit(y|ies) to a table within a storage account

        Args:
            entity: a row to insert
            table_name: a table name
        Returns:
            bool
        """
        
        assert table_name, "Please set your table name"
        assert type(entity) == list or type(entity) == dict, "Please set your entity"

        try:
            async with TableClient.from_connection_string(conn_str=self.connection_string,
                                                            table_name=table_name) as tClient:
                # single entity operation
                if type(entity) == dict or len(entity) == 1:
                    _ = await tClient.upsert_entity(mode=UpdateMode.MERGE, entity=entity)
                # batch mode
                else:
                    operations: List[TransactionOperationType] = self._format_batch_operation(entities=entity,
                                                                                                ctype='upsert')
                    _ = await tClient.submit_transaction(operations)

        except TableTransactionError as e:
            raise RuntimeError(f"Error while updating entity: {e}")
        except Exception as e:
            raise RuntimeError(f"{e}")

    async def delete_entity(self,
                            *,
                            entity:EntityType,
                            table_name:str) -> bool:
        """Delete entit(y|ies) to a table within a storage account
        
        Args:
            entity: a row to delete
            table_name: a table name
        Returns:
            bool
        """
        
        
        assert table_name, "Please set your table name"
        assert type(entity) == list or type(entity) == dict, "Please set your entity"

        try:
            async with TableClient.from_connection_string(conn_str=self.connection_string,
                                                            table_name=table_name) as tClient:
                # single entity operation
                if type(entity) == dict or len(entity) == 1:
                    _ = await tClient.delete_entity(entity=entity)
                # batch mode
                else:
                    operations: List[TransactionOperationType] = self._format_batch_operation(entities=entity,
                                                                                                ctype='delete')
                    _ = await tClient.submit_transaction(operations)

        except TableTransactionError as e:
            raise RuntimeError(f"Error while deleting entity: {e}")
        except Exception as e:
            raise RuntimeError(f"Error: {e}")

    async def query_entity(self,
                           *,
                           select:list,
                           parameters:dict,
                           name_filter:str,
                           table_name:str,) -> list:
        """Select a entity or multiple entities from a table within a storage account

        Args:
            
            
            select_statement: fields to return
            paramters: field value mapping as in field:value
            name_filter: filter statement
            table_name: a table name
            
        Returns:
            list(selected rows)
        """

        
        assert table_name, "Please set your table name"
        assert select, "Please set your select statement"
        assert parameters, "Please set your parameters"
        assert name_filter, "Please set your filter statement"

        try:
            async with TableClient.from_connection_string(conn_str=self.connection_string,
                                                        table_name=table_name) as tClient:
                
                qentity = await tClient.query_entities(query_filter=name_filter,
                                                        select=select,
                                                        parameters=parameters)
            
                return qentity
        
        except HttpResponseError as e:
            raise RuntimeError(f"Error while querying entity: {e}")
        except Exception as e:
            raise RuntimeError(f"{e}")