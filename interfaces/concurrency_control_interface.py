from abc import ABC, abstractmethod


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
    ) -> bool:
        """
        Ekuivalen dengan ccm.transaction_query().
        Request akses ke resource (tabel).
        """
        pass
    
    @abstractmethod
    def check_deadlock(self, transaction_id: int) -> bool:
        pass
    
    @abstractmethod
    def get_transaction_status(self, transaction_id: int) -> str:
        pass
