from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

from StorageManager.classes.DataModels import DataDeletion, DataRetrieval, DataWrite, Schema
from ..models import Rows

class AbstractStorageManager(ABC):

    @abstractmethod
    def read_block(self, data_retrieval: DataRetrieval) -> Rows:
        pass
    
    @abstractmethod
    def write_block(self, data_write: DataWrite) -> int:
        pass

    @abstractmethod
    def delete_block(self, data_deletion: DataDeletion) -> int:
        pass
    
    @abstractmethod
    def create_table(self, table_name: str, schema: Schema) -> bool:
        pass
    
    @abstractmethod
    def drop_table(self, table_name: str) -> bool:
        pass