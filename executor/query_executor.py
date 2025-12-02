import time
from datetime import datetime
from typing import Optional
from ..models import (
    QueryPlan,
    ExecutionResult,
    Transaction,
    TableScanNode,
    InsertPlan,
    UpdatePlan,
    DeletePlan,
    CreateTablePlan,
    DropTablePlan
)
from ..interfaces import (
    AbstractStorageManager,
    AbstractConcurrencyControlManager,
    AbstractFailureRecoveryManager
)
from .execution_visitor import ExecutionVisitor


class QueryExecutor:
    
    def __init__(
        self,
        storage_manager: AbstractStorageManager,
        concurrency_manager: Optional[AbstractConcurrencyControlManager] = None,
        failure_recovery: Optional[AbstractFailureRecoveryManager] = None
    ):
        self.storage_manager = storage_manager
        self.concurrency_manager = concurrency_manager
        self.recovery_manager = failure_recovery 
        self.current_transaction: Optional[int] = None
    
    def execute(self, plan: QueryPlan, transaction: Optional[Transaction] = None) -> ExecutionResult:
        transaction_id = transaction.transaction_id if transaction else None
        visitor = ExecutionVisitor(
            self.storage_manager,
            self.concurrency_manager,
            transaction_id,
            self.recovery_manager
        )
        
        try:
            result = plan.accept(visitor)
            
            if isinstance(result, ExecutionResult):
                return result
            
            return ExecutionResult(
                success=True,
                rows=result,
                affected_rows=len(result.data) if result else 0,
                message="Query executed successfully"
            )
        except Exception as e:
            return ExecutionResult(
                success=False,
                error=str(e),
                message=f"Query execution failed: {str(e)}"
            )
    
    def execute_with_transaction(self, plan: QueryPlan) -> ExecutionResult:
        transaction_id: Optional[int] = None
        
        try:
            if self.concurrency_manager:
                transaction_id = self.concurrency_manager.begin_transaction()
                self.current_transaction = transaction_id
                # print(f"Transaction {transaction_id} started")
            
            result = self.execute(plan, Transaction(transaction_id))
            
            if not result.success:
                raise Exception(result.error)
            
            if self.concurrency_manager and transaction_id:
                success = self._flush_to_storage(result)
                
                if success:
                    self.concurrency_manager.commit_transaction(transaction_id)
                    self.concurrency_manager.commit_flushed(transaction_id)
                    self.concurrency_manager.end_transaction(transaction_id)
                    print(f"Transaction {transaction_id} committed successfully")
                else:
                    raise Exception("Failed to flush to storage")
            
            self.current_transaction = None
            return result
            
        except Exception as e:
            if self.concurrency_manager and transaction_id:
                print(f"Rolling back transaction {transaction_id}")
                self.concurrency_manager.rollback_transaction(transaction_id)
                self.concurrency_manager.end_transaction(transaction_id)
            
            self.current_transaction = None
            return ExecutionResult(
                success=False,
                error=str(e),
                message=f"Transaction failed: {str(e)}"
            )
    
    def _acquire_locks(self, plan: QueryPlan, lock_type: str) -> bool:
        return True
    
    def _extract_tables(self, plan: QueryPlan) -> list:
        
        tables = []
        
        if isinstance(plan, (TableScanNode, InsertPlan, UpdatePlan, DeletePlan, CreateTablePlan, DropTablePlan)):
            tables.append(plan.table_name)
        
        # Recursive untuk child nodes
        if hasattr(plan, 'child') and plan.child:
            tables.extend(self._extract_tables(plan.child))
        if hasattr(plan, 'left_child') and plan.left_child:
            tables.extend(self._extract_tables(plan.left_child))
        if hasattr(plan, 'right_child') and plan.right_child:
            tables.extend(self._extract_tables(plan.right_child))
        
        return list(set(tables))
    
    def _flush_to_storage(self, result: ExecutionResult) -> bool:
        """Flush result ke storage (untuk write operations)"""
        # TODO: Implement flush logic
        return True
