from datetime import datetime
from typing import Optional
from .rows import Rows


class ExecutionResult:
    
    def __init__(
        self,
        success: bool,
        data: Optional[Rows] = None,
        affected_rows: int = 0,
        message: str = "",
        error: Optional[str] = None,
        execution_time: float = 0.0,
        transaction_id: Optional[str] = None,
        query: Optional[str] = None
    ):
        self.transaction_id = transaction_id
        self.timestamp = datetime.now()
        self.message = message
        self.data = data
        self.query = query
        self.affected_rows = affected_rows

        # Atribut Tambahan
        self.error = error
        self.success = success
        self.execution_time = execution_time
    
    def __repr__(self) -> str:
        if self.success:
            tx_info = f", tx={self.transaction_id}" if self.transaction_id else ""
            return f"ExecutionResult(success=True, affected_rows={self.affected_rows}, message='{self.message}'{tx_info})"
        else:
            return f"ExecutionResult(success=False, error='{self.error}')"