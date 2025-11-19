from models.execution_result import ExecutionResult
from models.recover_criteria import RecoverCriteria

class FailureRecovery:
    # TODO: Define atribut (Buat jadi static, jangan pakai self), sesuai spesifikasi
    # TODO: Masukkan helper class (misalnya RecoverCriteria) ke dalam folder models/recover_criteria.py
    
    @staticmethod
    def write_log(info: ExecutionResult) -> None:
        pass
    
    @staticmethod
    def recover(criteria: RecoverCriteria) -> None:
        pass

    @staticmethod
    def save_checkpoint() -> None:
        pass
