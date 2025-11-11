# STUB Implementation untuk demo Query Processor
# Parser dan optimizer sebenarnya harus diimplementasikan oleh grup QO

from typing import List
from ..interfaces import AbstractQueryOptimizer
from ..models import (
    QueryPlan,
    TableScanNode,
    FilterNode,
    ProjectNode,
    SortNode,
    NestedLoopJoinNode,
    InsertPlan,
    UpdatePlan,
    DeletePlan,
    CreateTablePlan,
    DropTablePlan
)


class SimpleQueryOptimizer(AbstractQueryOptimizer):
    
    def __init__(self):
        pass
    
    def optimize(self, query: str) -> QueryPlan:
        # Stub: matching string sederhana, bukan parser sebenarnya
        query_lower = query.lower().strip()
        
        if query_lower.startswith("select"):
            return self._stub_select_plan()
        elif query_lower.startswith("insert"):
            return self._stub_insert_plan()
        elif query_lower.startswith("update"):
            return self._stub_update_plan()
        elif query_lower.startswith("delete"):
            return self._stub_delete_plan()
        elif query_lower.startswith("create"):
            return self._stub_create_table_plan()
        elif query_lower.startswith("drop"):
            return self._stub_drop_table_plan()
        else:
            raise ValueError(f"Unsupported query type")
    
    def _stub_select_plan(self) -> QueryPlan:
        from ..models.conditions import WhereCondition, ComparisonOperator, OrderByClause
        
        # Hardcoded: SELECT name, age FROM users WHERE age > 25 ORDER BY name
        scan = TableScanNode("users")
        filter_node = FilterNode(scan, WhereCondition("age", ComparisonOperator.GREATER_THAN, 25))
        project = ProjectNode(filter_node, ["name", "age"])
        sort = SortNode(project, [OrderByClause("name", "ASC")], None)
        
        # TODO: grup QO harus set cost estimates
        return sort
    
    def _stub_insert_plan(self) -> InsertPlan:
        # Hardcoded: INSERT INTO users (name, age) VALUES ('Alice', 30)
        plan = InsertPlan(
            table_name="users",
            columns=["name", "age"],
            values=["Alice", 30]
        )
        return plan
    
    def _stub_update_plan(self) -> UpdatePlan:
        from ..models.conditions import WhereCondition, ComparisonOperator
        
        # Hardcoded: UPDATE users SET age = 31 WHERE name = 'Alice'
        plan = UpdatePlan(
            table_name="users",
            set_clause={"age": 31},
            where=WhereCondition("name", ComparisonOperator.EQUALS, "Alice")
        )
        return plan
    
    def _stub_delete_plan(self) -> DeletePlan:
        from ..models.conditions import WhereCondition, ComparisonOperator
        
        # Hardcoded: DELETE FROM users WHERE age < 18
        plan = DeletePlan(
            table_name="users",
            where=WhereCondition("age", ComparisonOperator.LESS_THAN, 18)
        )
        return plan
    
    def _stub_create_table_plan(self) -> CreateTablePlan:
        # Hardcoded: CREATE TABLE products (id INT, name VARCHAR, price DECIMAL)
        plan = CreateTablePlan(
            table_name="products",
            schema={"id": "INT", "name": "VARCHAR", "price": "DECIMAL"}
        )
        return plan
    
    def _stub_drop_table_plan(self) -> DropTablePlan:
        # Hardcoded: DROP TABLE old_data
        plan = DropTablePlan(table_name="old_data")
        return plan