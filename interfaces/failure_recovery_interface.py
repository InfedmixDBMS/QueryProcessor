from abc import ABC, abstractmethod
from typing import Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models import ExecutionResult


class AbstractFailureRecoveryManager(ABC):
    
    @abstractmethod
    def write_log(self, execution_result: 'ExecutionResult', table: Optional[str] = None, 
                  key: Optional[Any] = None, old_value: Optional[Any] = None, 
                  new_value: Optional[Any] = None) -> None:
        pass
    
    @abstractmethod
    def log_transaction_start(self, transaction_id: int) -> None:
        pass
    
    @abstractmethod
    def log_transaction_commit(self, transaction_id: int) -> None:
        pass
    
    @abstractmethod
    def log_transaction_abort(self, transaction_id: int) -> None:
        pass
    
    @abstractmethod
    def recover(self) -> bool:
        pass