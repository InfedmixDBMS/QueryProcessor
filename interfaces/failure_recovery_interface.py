from abc import ABC, abstractmethod
from models.execution_result import ExecutionResult
from models.recover_criteria import RecoverCriteria


class AbstractFailureRecoveryManager(ABC):
    
    @abstractmethod
    def write_log(info: ExecutionResult) -> None:
        pass

    @abstractmethod
    def save_checkpoint() -> None:
        pass
    
    @abstractmethod
    def recover(criteria: RecoverCriteria) -> None:
        pass