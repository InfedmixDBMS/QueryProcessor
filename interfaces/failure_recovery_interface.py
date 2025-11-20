from abc import ABC, abstractmethod


class AbstractFailureRecoveryManager(ABC):
    
    @abstractmethod
    def log_query(self, transaction_id: int, query: str) -> None:
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
