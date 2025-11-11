from datetime import datetime
from ..models import (
    Rows,
    ExecutionResult,
    Transaction,
    QueryPlan
)
from ..interfaces import AbstractStorageManager, AbstractConcurrencyControlManager
from .execution_visitor import ExecutionVisitor


class QueryExecutor:
    
    def __init__(
        self,
        storage_manager: AbstractStorageManager,
        concurrency_manager: AbstractConcurrencyControlManager
    ):
        self.storage_manager = storage_manager
        self.concurrency_manager = concurrency_manager
    
    def execute(
        self,
        query_plan: QueryPlan,
        transaction: Transaction
    ) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            # Cek deadlock
            if self.concurrency_manager.check_deadlock(transaction.transaction_id):
                return ExecutionResult(
                    success=False,
                    error="Deadlock detected. Transaction aborted.",
                    transaction_id=transaction.transaction_id
                )
            
            # Buat execution visitor
            visitor = ExecutionVisitor(
                storage_manager=self.storage_manager,
                concurrency_manager=self.concurrency_manager,
                transaction_id=transaction.transaction_id
            )
            
            # Eksekusi menggunakan visitor pattern
            result = query_plan.accept(visitor)
            
            # Handle result type: ExecutionResult atau Rows
            if isinstance(result, ExecutionResult):
                return result
            elif isinstance(result, Rows):
                execution_time = (datetime.now() - start_time).total_seconds()
                return ExecutionResult(
                    success=True,
                    rows=result,
                    message=f"Retrieved {result.row_count} row(s)",
                    execution_time=execution_time,
                    transaction_id=transaction.transaction_id
                )
            else:
                raise ValueError(f"Unexpected result type from visitor: {type(result)}")
        
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                error=f"Execution error: {str(e)}",
                execution_time=execution_time,
                transaction_id=transaction.transaction_id
            )
