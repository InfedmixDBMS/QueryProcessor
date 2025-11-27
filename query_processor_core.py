import threading
from datetime import datetime
from typing import Dict, Optional
from .models import ExecutionResult, Transaction
from .interfaces import (
    AbstractQueryOptimizer,
    AbstractStorageManager,
    AbstractConcurrencyControlManager,
    AbstractFailureRecoveryManager
)
from .executor import QueryExecutor


class QueryProcessor:
    
    def __init__(
        self,
        optimizer: AbstractQueryOptimizer,
        storage_manager: AbstractStorageManager,
        concurrency_manager: AbstractConcurrencyControlManager,
        recovery_manager: AbstractFailureRecoveryManager
    ):
        self.optimizer = optimizer
        self.storage_manager = storage_manager
        self.concurrency_manager = concurrency_manager
        self.recovery_manager = recovery_manager
        self.executor = QueryExecutor(storage_manager, concurrency_manager, recovery_manager)
        self.active_transactions: Dict[int, Transaction] = {}
        self._lock = threading.Lock()

    def begin_transaction(self) -> int:
        transaction_id = self.concurrency_manager.begin_transaction()
        with self._lock:
            self._get_or_create_transaction(transaction_id)
        return transaction_id
    
    def get_optimizer(self) -> AbstractQueryOptimizer:
        return self.optimizer
    
    def get_storage_manager(self) -> AbstractStorageManager:
        return self.storage_manager
    
    def get_concurrency_manager(self) -> AbstractConcurrencyControlManager:
        return self.concurrency_manager
    
    def get_recovery_manager(self) -> AbstractFailureRecoveryManager:
        return self.recovery_manager
    
    def get_executor(self) -> QueryExecutor:
        return self.executor
    
    def execute_query(self, query: str, transaction_id: Optional[int] = None) -> ExecutionResult:
        try:

            plan = self.optimizer.optimize(query)
            
            if isinstance(plan, ExecutionResult):
                return plan

            txn_obj = None
            if transaction_id is not None:
                txn_obj = Transaction(transaction_id)

            result = self.executor.execute(plan, txn_obj)
            
            return result

        except Exception as e:
            return ExecutionResult(
                success=False, 
                error=str(e),
                message=f"Error executing query: {str(e)}"
            )
    
    def _get_or_create_transaction(self, transaction_id: Optional[int]) -> Transaction:
        if transaction_id and (transaction_id in self.active_transactions):
            return self.active_transactions[transaction_id]
        else:
            transaction = Transaction(transaction_id)
            self.active_transactions[transaction.transaction_id] = transaction
            self.recovery_manager.log_transaction_start(transaction.transaction_id)
            return transaction
    
    def _calculate_execution_time(self, start_time: datetime) -> float:
        return (datetime.now() - start_time).total_seconds()
    
    def _request_lock(
        self, 
        transaction: Transaction, 
        resource_id: str, 
        lock_type: str
    ) -> bool:
        granted = self.concurrency_manager.request_lock(
            transaction.transaction_id,
            resource_id,
            lock_type
        )
        
        if granted:
            transaction.add_lock(resource_id)
        
        return granted
    
    def _abort_transaction(self, transaction: Transaction) -> None:
        transaction.abort()
        self.concurrency_manager.rollback_transaction(transaction.transaction_id)
        self.recovery_manager.log_transaction_abort(transaction.transaction_id)
    
    def commit_transaction(self, transaction_id: int) -> ExecutionResult:
        with self._lock:
            if transaction_id not in self.active_transactions:
                return ExecutionResult(
                    success=False,
                    error=f"Transaction '{transaction_id}' not found"
                )
            
            transaction = self.active_transactions[transaction_id]
            
            try:
                transaction.commit()
                
                # Commit di Concurrency Manager
                if not self.concurrency_manager.commit_transaction(transaction_id):
                    return ExecutionResult(
                        success=False,
                        error=f"Failed to commit transaction {transaction_id} in concurrency manager"
                    )
                
                self.concurrency_manager.commit_flushed(transaction_id)
                self.concurrency_manager.end_transaction(transaction_id)
                
                # Log commit
                self.recovery_manager.log_transaction_commit(transaction_id)
                
                # Hapus dari active transactions
                del self.active_transactions[transaction_id]
                
                return ExecutionResult(
                    success=True,
                    message=f"Transaction '{transaction_id}' committed successfully"
                )
                
            except Exception as e:
                return ExecutionResult(
                    success=False,
                    error=f"Failed to commit transaction: {str(e)}"
                )
    
    def rollback_transaction(self, transaction_id: int) -> ExecutionResult:
        with self._lock:
            if transaction_id not in self.active_transactions:
                return ExecutionResult(
                    success=False,
                    error=f"Transaction '{transaction_id}' not found"
                )
            
            transaction = self.active_transactions[transaction_id]
            
            try:
                # Abort transaction
                self._abort_transaction(transaction)
                
                # Hapus dari active transactions
                del self.active_transactions[transaction_id]
                
                return ExecutionResult(
                    success=True,
                    message=f"Transaction '{transaction_id}' rolled back successfully"
                )
                
            except Exception as e:
                return ExecutionResult(
                    success=False,
                    error=f"Failed to rollback transaction: {str(e)}"
                )
