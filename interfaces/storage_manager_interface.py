from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

from StorageManager.classes.DataModels import DataDeletion, DataRetrieval, DataWrite, Schema
from ..models import Rows

class AbstractStorageManager(ABC):
    
    # DataRetrieval: 
    #   table -> str, 
    #   column -> List[str], 
    #   condition -> List[Condition]

    # Condition: 
    #   column -> str, 
    #   operation -> enum('=', '<>', '>', '<', '>=', '<='), 
    #   operand -> int | str | float

    @abstractmethod
    def read_block(self, data_retrieval: DataRetrieval) -> Rows:
        pass
    
    # DataWrite: 
    #   table -> str,
    #   column -> List[str],
    #   conditions -> List[Condition],
    #   new_value -> List[T] | None
    
    @abstractmethod
    def write_block(self, data_write: DataWrite) -> int:
        pass
    
    # DataDeletion: 
    #   table -> str,
    #   conditions -> List[Condition]
    @abstractmethod
    def delete_block(self, data_deletion: DataDeletion) -> int:
        pass

    # Schema:
    #   columns:
    #       "id" = IntType(),
    #       "name" = VarCharType(50),
    #       "ipk" = FloatType()
    #   column_order: list(columns.keys())
    #   column_index: {name: i for i, name in enumerate(self.column_order)}
    #   size: int (auto-calculated, ga perlu dihitung manual)
    
    @abstractmethod
    def create_table(self, table_name: str, schema: Schema) -> bool:
        pass
    
    @abstractmethod
    def drop_table(self, table_name: str) -> bool:
        pass