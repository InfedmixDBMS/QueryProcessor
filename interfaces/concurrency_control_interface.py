from abc import ABC, abstractmethod


class AbstractConcurrencyControlManager(ABC):
    
    @abstractmethod
    def request_lock(
        self, 
        transaction_id: str, 
        resource_id: str, 
        lock_type: str
    ) -> bool:
        pass
    
    @abstractmethod
    def release_lock(self, transaction_id: str, resource_id: str) -> bool:
        pass
    
    @abstractmethod
    def release_all_locks(self, transaction_id: str) -> bool:
        pass
    
    @abstractmethod
    def check_deadlock(self, transaction_id: str) -> bool:
        pass
