from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class LockResult:
    granted: bool
    status: str  # 'GRANTED', 'WAITING', 'FAILED'
    wait_time: float = 0.5    # Suggested wait time in seconds
    blocked_by: list = None 
    active_transactions: list = None
    message: str = ""

class AbstractConcurrencyControlManager(ABC):
    
    @abstractmethod
    def begin_transaction(self) -> int:
        pass
    
    @abstractmethod
    def commit_transaction(self, transaction_id: int) -> bool:
        pass
    
    @abstractmethod
    def commit_flushed(self, transaction_id: int) -> bool:
        pass
    
    @abstractmethod
    def rollback_transaction(self, transaction_id: int) -> bool:
        pass
    
    @abstractmethod
    def end_transaction(self, transaction_id: int) -> bool:
        pass
    
    @abstractmethod
    def request_lock(
        self, 
        transaction_id: int, 
        resource_id: str, 
        lock_type: str
    ) -> LockResult:
        """
        Ekuivalen dengan ccm.transaction_query().
        Request akses ke resource (tabel).
        Return LockResult: success status dan resolution strategy.
        """
        pass
    
    @abstractmethod
    def check_deadlock(self, transaction_id: int) -> bool:
        pass
    
    @abstractmethod
    def get_transaction_status(self, transaction_id: int) -> str:
        pass
