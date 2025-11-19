# TODO: FailureRecoveryManager tolong disimpan di folder components
from components import FailureRecoveryManager

from models.execution_result import ExecutionResult

# TODO: RecoverCriteria tolong disimpan di models/recover_criteria.py
from models.recover_criteria import RecoverCriteria


class AbstractFailureRecoveryManager(FailureRecoveryManager):
    
    def write_log(info: ExecutionResult) -> None:
        pass

    def save_checkpoint() -> None:
        pass
    
    def recover(criteria: RecoverCriteria) -> None:
        pass