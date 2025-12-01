from datetime import datetime
from typing import Any, Dict, Optional, List
from .plan_visitor import QueryPlanVisitor
from ..models import (
    Rows,
    ExecutionResult,
    TableScanNode,
    FilterNode,
    ProjectNode,
    SortNode,
    NestedLoopJoinNode,
    InsertPlan,
    UpdatePlan,
    DeletePlan,
    CreateTablePlan,
    DropTablePlan,
    WhereCondition,
    ComparisonOperator
)
from ..interfaces import AbstractStorageManager, AbstractConcurrencyControlManager, AbstractFailureRecoveryManager

try:
    from StorageManager.classes.DataModels import DataDeletion, DataRetrieval, DataWrite, Schema, Condition, Operation
    from StorageManager.classes.Types import IntType, VarCharType, FloatType
except ImportError:
    # Bikin Struct Sendiri
    class DataRetrieval:
        def __init__(self, table: str, column: List[str], condition: List['Condition'] = None):
            self.table = table
            self.column = column
            self.condition = condition or []
    
    class DataWrite:
        def __init__(self, table: str, column: List[str], conditions: List['Condition'] = None, new_value: List[Any] = None):
            self.table = table
            self.column = column
            self.conditions = conditions or []
            self.new_value = new_value or []
    
    class DataDeletion:
        def __init__(self, table: str, conditions: List['Condition']):
            self.table = table
            self.conditions = conditions
    
    class Schema:
        def __init__(self, columns: Dict[str, Any]):
            self.columns = columns
            self.column_order = list(columns.keys())
            self.column_index = {name: i for i, name in enumerate(self.column_order)}
            self.size = len(columns)
    
    class Condition:
        def __init__(self, column: str, operation: str, operand: Any):
            self.column = column
            self.operation = operation
            self.operand = operand
    
    class IntType:
        pass
    
    class VarCharType:
        def __init__(self, length: int):
            self.length = length
    
    class FloatType:
        pass

import time


class ExecutionVisitor(QueryPlanVisitor):
    def __init__(
        self,
        storage_manager: AbstractStorageManager,
        concurrency_manager: Optional[AbstractConcurrencyControlManager] = None,
        current_transaction: Optional[int] = None,
        failure_recovery_manager: Optional[AbstractFailureRecoveryManager] = None
    ):
        self.storage_manager = storage_manager
        self.concurrency_manager = concurrency_manager
        self.current_transaction = current_transaction
        self.failure_recovery_manager = failure_recovery_manager
    
    def _request_lock_with_wait(self, resource_id: str, lock_type: str) -> bool:
        if not self.concurrency_manager or self.current_transaction is None:
            return True

        max_retries = 10
        retry_count = 0

        while retry_count < max_retries:
            result = self.concurrency_manager.request_lock(
                self.current_transaction,
                resource_id,
                lock_type
            )

            if result.granted:
                return True

            if result.status == "WAITING":
                time.sleep(result.wait_time)
                retry_count += 1
            elif result.status == "FAILED":
                raise RuntimeError(f"Lock denied for {resource_id}: {result.status}")
            else:
                return False
        
        raise RuntimeError(f"Timeout waiting for lock on {resource_id}")

    def visit_table_scan(self, node: TableScanNode) -> Rows:
        self._request_lock_with_wait(node.table_name, "READ")
        
        # Buat awal doang, ambil SEMUA KOLOM dan SEMUA ROW
        data_retrieval = DataRetrieval(
            table=node.table_name,
            column=[],
            conditions=[]
        )
        
        return self.storage_manager.read_block(data_retrieval)
    
    def visit_filter(self, node: FilterNode) -> Rows:
        if hasattr(node.child, 'table_name') and isinstance(node.child, TableScanNode):
            table_name = node.child.table_name
            self._request_lock_with_wait(table_name, "READ")
            
            storage_conditions = self._convert_where_to_storage_conditions([node.condition])
            
            data_retrieval = DataRetrieval(
                table=table_name,
                column=[],
                conditions=storage_conditions
            )
            
            return self.storage_manager.read_block(data_retrieval)
        
        # TOLONG REVIEW INI
        else:
            child_rows = node.child.accept(self)
            
            filtered_data = []
            for row in child_rows.data:
                row_dict = {col: val for col, val in zip(child_rows.columns, row)}
                
                if node.condition.evaluate(row_dict):
                    filtered_data.append(row)
            
            return Rows(child_rows.columns, filtered_data)
    
    def visit_project(self, node: ProjectNode) -> Rows:
        if (hasattr(node.child, 'table_name') and isinstance(node.child, TableScanNode) 
            and '*' not in node.columns):

            table_name = node.child.table_name
            
            # TOLONG REVIEW INI
            self._request_lock_with_wait(table_name, "READ")
            
            data_retrieval = DataRetrieval(
                table=table_name,
                column=node.columns,
                conditions=[]
            )
            
            return self.storage_manager.read_block(data_retrieval)
        
        # TOLONG REVIEW INI
        else:
            child_rows = node.child.accept(self)
            
            if '*' in node.columns:
                return child_rows
            
            try:
                column_indices = [child_rows.columns.index(col) for col in node.columns]
            except ValueError as e:
                raise ValueError(f"Column not found during projection: {e}")
            
            projected_data = []
            for row in child_rows.data:
                projected_row = [row[idx] for idx in column_indices]
                projected_data.append(projected_row)
            
            return Rows(node.columns, projected_data)
    
    def visit_sort(self, node: SortNode) -> Rows:
        child_rows = node.child.accept(self)
        
        if not node.order_by:
            if node.limit:
                return Rows(child_rows.columns, child_rows.data[:node.limit])
            return child_rows
        
        sort_indices = []
        for clause in node.order_by:
            try:
                idx = child_rows.columns.index(clause.column)
                sort_indices.append((idx, clause.direction))
            except ValueError:
                raise ValueError(f"ORDER BY column not found: {clause.column}")
        
        def sort_key(row):
            keys = []
            for idx, direction in sort_indices:
                value = row[idx]
                if value is None:
                    value = ""
                
                if direction == "DESC":
                    if isinstance(value, (int, float)):
                        keys.append(-value)
                    else:
                        keys.append(tuple(-ord(c) if c else 0 for c in str(value)))
                else:
                    if isinstance(value, str):
                        keys.append(tuple(ord(c) for c in value))
                    else:
                        keys.append(value)
            return tuple(keys)
        
        sorted_data = sorted(child_rows.data, key=sort_key)
        
        if node.limit:
            sorted_data = sorted_data[:node.limit]
        
        return Rows(child_rows.columns, sorted_data)
    
    def visit_join(self, node: NestedLoopJoinNode) -> Rows:
        left_rows = node.left_child.accept(self)
        right_rows = node.right_child.accept(self)
        
        result_columns = left_rows.columns + right_rows.columns
        result_data = []
        
        for left_row in left_rows.data:
            for right_row in right_rows.data:
                combined_dict = {}
                for col, val in zip(left_rows.columns, left_row):
                    combined_dict[col] = val
                for col, val in zip(right_rows.columns, right_row):
                    combined_dict[col] = val
                
                if node.join_condition.condition.evaluate(combined_dict):
                    combined_row = list(left_row) + list(right_row)
                    result_data.append(combined_row)
        
        return Rows(result_columns, result_data)
    
    
    def visit_insert(self, plan: InsertPlan) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            self._request_lock_with_wait(plan.table_name, "WRITE")
            
            columns = plan.columns
            if not columns:
                all_columns = self.storage_manager.load_schema_names(plan.table_name)
                columns = [col for col in all_columns if col != '__row_id']
            
            if len(columns) != len(plan.values):
                raise Exception(f"Column count ({len(columns)}) does not match value count ({len(plan.values)})")
            
            if self.failure_recovery_manager and self.current_transaction:
                predicted_row_id = self.storage_manager.get_next_row_id(plan.table_name)
                
                #new_value_dict = dict(zip(columns, plan.values))
                
                log_exec_result = ExecutionResult(
                    success=True,
                    transaction_id=self.current_transaction,
                    query=f"INSERT INTO {plan.table_name}"
                )
                self.failure_recovery_manager.write_log(
                    log_exec_result, 
                    table=plan.table_name, 
                    key=predicted_row_id, 
                    old_value=None, 
                    new_value=dict(zip(columns, plan.values))
                )
            
            data_write = DataWrite(
                table=plan.table_name,
                column=columns,
                conditions=[],
                new_value=[plan.values]
            )
            
            inserted_rows = self.storage_manager.write_block(data_write)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return ExecutionResult(
                success=True,
                affected_rows=inserted_rows,
                message=f"Inserted {inserted_rows} row(s) into {plan.table_name}",
                execution_time=execution_time,
                transaction_id=self.current_transaction,
                query=f"INSERT INTO {plan.table_name} VALUES ..."
            )
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                error=f"Insert failed: {str(e)}",
                execution_time=execution_time,
                transaction_id=self.current_transaction
            )
    
    def visit_update(self, plan: UpdatePlan) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            self._request_lock_with_wait(plan.table_name, "WRITE")
            
            storage_conditions = []
            if plan.where:
                storage_conditions = self._convert_where_to_storage_conditions([plan.where])
            
            if self.failure_recovery_manager and self.current_transaction:
                data_retrieval = DataRetrieval(
                    table=plan.table_name,
                    column=[],
                    conditions=storage_conditions
                )
                old_rows = self.storage_manager.read_block(data_retrieval)
                
                if old_rows and old_rows.data:
                    all_columns = self.storage_manager.load_schema_names(plan.table_name)
                    all_columns = [col for col in all_columns if col != '__row_id'] 

                    updated_rows = []
                    for old_row in old_rows.data:
                        old_row_dict = dict(zip(old_rows.columns, old_row))
            
                        new_row_dict = {}
                        for col in all_columns:
                            if col in plan.set_clause:
                                new_row_dict[col] = plan.set_clause[col]
                            elif col in old_row_dict:
                                new_row_dict[col] = old_row_dict[col]
                            else:
                                new_row_dict[col] = None
                        
                        new_row_values = [new_row_dict[col] for col in all_columns]
                        updated_rows.append(new_row_values)
                        
                        if self.failure_recovery_manager and self.current_transaction:
                            if '__row_id' in old_row_dict:
                                row_id = old_row_dict['__row_id']
                            else:
                                row_id = old_row[0]  
                            
                            log_exec_result = ExecutionResult(
                                success=True,
                                transaction_id=self.current_transaction,
                                query=f"UPDATE {plan.table_name}"
                            )
                            
                            self.failure_recovery_manager.write_log(
                                log_exec_result, 
                                table=plan.table_name, 
                                key=row_id, 
                                old_value=old_row_dict, 
                                new_value=plan.set_clause
                            )
            
            data_deletion = DataDeletion(
                table=plan.table_name,
                conditions=storage_conditions
            )
            deleted_count = self.storage_manager.delete_block(data_deletion)
            
            data_write = DataWrite(
                table=plan.table_name,
                column=all_columns,  
                conditions=[],
                new_value=updated_rows  
            )
            
            affected_rows = self.storage_manager.write_block(data_write)
            # TOLONG REVIEW INI, APAKAH SETELAHNYA PERLU delete_block?
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            where_desc = f" WHERE {plan.where}" if plan.where else ""
            return ExecutionResult(
                success=True,
                affected_rows=affected_rows,
                message=f"Updated {affected_rows} row(s) in {plan.table_name}{where_desc}",
                execution_time=execution_time,
                transaction_id=self.current_transaction,
                query=f"UPDATE {plan.table_name} SET ..."
            )
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                error=f"Update failed: {str(e)}",
                execution_time=execution_time,
                transaction_id=self.current_transaction
            )
    
    def visit_delete(self, plan: DeletePlan) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            self._request_lock_with_wait(plan.table_name, "WRITE")
            
            storage_conditions = []
            if plan.where:
                storage_conditions = self._convert_where_to_storage_conditions([plan.where])

            # TODO: FIX setelah main aman
            old_rows = self.storage_manager.read_block
            if self.failure_recovery_manager and self.current_transaction and old_rows:
                for old_row in old_rows.data:
                    if '__row_id' in old_rows.columns:
                        row_id = old_row[old_rows.columns.index('__row_id')]
                    else:
                        row_id = old_row[0]
                
                    old_value_dict = dict(zip(old_rows.columns, old_row))
                    
                    frm_exec_result = ExecutionResult(
                        success=True,
                        transaction_id=self.current_transaction,
                        query=f"DELETE FROM {plan.table_name}",
                    )

                    self.failure_recovery_manager.write_log(frm_exec_result, plan.table_name, row_id, old_value_dict, None)
            
            data_deletion = DataDeletion(
                table=plan.table_name,
                conditions=storage_conditions
            )
            
            deleted_rows = self.storage_manager.delete_block(data_deletion)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            where_desc = f" WHERE {plan.where}" if plan.where else " (all rows)"
            return ExecutionResult(
                success=True,
                affected_rows=deleted_rows,
                message=f"Deleted {deleted_rows} row(s) from {plan.table_name}{where_desc}",
                execution_time=execution_time,
                transaction_id=self.current_transaction,
                query=f"DELETE FROM {plan.table_name} ..."
            )
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                error=f"Delete failed: {str(e)}",
                execution_time=execution_time,
                transaction_id=self.current_transaction
            )
    
    # Operasi DDL
    
    def visit_create_table(self, plan: CreateTablePlan) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            schema_columns = {}
            for column_name, column_type in plan.schema.items():
                schema_columns[column_name] = self._convert_to_storage_type(column_type)
            
            schema = Schema(**schema_columns)
            
            success = self.storage_manager.create_table(
                plan.table_name,
                schema
            )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            if success:
                return ExecutionResult(
                    success=True,
                    message=f"Table '{plan.table_name}' created successfully with {len(plan.schema)} columns",
                    execution_time=execution_time,
                    transaction_id=self.current_transaction,
                    query=f"CREATE TABLE {plan.table_name} ..."
                )
            else:
                return ExecutionResult(
                    success=False,
                    error=f"Failed to create table '{plan.table_name}'",
                    execution_time=execution_time,
                    transaction_id=self.current_transaction
                )
                
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                error=f"Create table failed: {str(e)}",
                execution_time=execution_time,
                transaction_id=self.current_transaction
            )
    
    def visit_drop_table(self, plan: DropTablePlan) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            success = self.storage_manager.drop_table(plan.table_name)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            if success:
                return ExecutionResult(
                    success=True,
                    message=f"Table '{plan.table_name}' dropped successfully",
                    execution_time=execution_time,
                    transaction_id=self.current_transaction,
                    query=f"DROP TABLE {plan.table_name}"
                )
            else:
                return ExecutionResult(
                    success=False,
                    error=f"Failed to drop table '{plan.table_name}'",
                    execution_time=execution_time,
                    transaction_id=self.current_transaction
                )
                
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                error=f"Drop table failed: {str(e)}",
                execution_time=execution_time,
                transaction_id=self.current_transaction
            )
    
    # Helper methods
    
    def _convert_where_to_storage_conditions(self, where_conditions: List[WhereCondition]) -> List[Condition]:
        storage_conditions = []
        
        for where_cond in where_conditions:
            operation_map = {
                ComparisonOperator.EQUALS: '=',
                ComparisonOperator.NOT_EQUALS: '<>',
                ComparisonOperator.GREATER_THAN: '>',
                ComparisonOperator.LESS_THAN: '<',
                ComparisonOperator.GREATER_EQUAL: '>=',
                ComparisonOperator.LESS_EQUAL: '<='
            }
            
            operation = operation_map.get(where_cond.operator, '=')
            
            storage_condition = Condition(
                column=where_cond.column,
                operation=operation,
                operand=where_cond.value
            )
            storage_conditions.append(storage_condition)
        
        return storage_conditions
    
    def _convert_to_storage_type(self, column_type: str):
        column_type = column_type.upper()
        
        if column_type == 'INT' or column_type == 'INTEGER':
            return IntType()
        elif column_type.startswith('VARCHAR'):
            if '(' in column_type and ')' in column_type:
                length_str = column_type[column_type.find('(')+1:column_type.find(')')]
                try:
                    length = int(length_str)
                    return VarCharType(length)
                except ValueError:
                    return VarCharType(255) 
            else:
                return VarCharType(255)
        elif column_type == 'FLOAT' or column_type == 'DOUBLE' or column_type == 'DECIMAL':
            return FloatType()
        else:
            return VarCharType(255)
    
    def _convert_where_to_dict(self, where_condition) -> Dict[str, Any]:
        return {
            where_condition.column: where_condition.value
        }