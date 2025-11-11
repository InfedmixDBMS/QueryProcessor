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


class QueryOptimizer(AbstractQueryOptimizer):
    
    def __init__(self):
        pass
    
    def optimize(self, query: str) -> QueryPlan:
        pass