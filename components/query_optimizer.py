from models.parsed_query import ParsedQuery

class OptimizationEngine:
    # TODO: Define atribut (Buat jadi static, jangan pakai self), sesuai spesifikasi
    # TODO: Masukkan semua helper class ke dalam folder models -> parsed_query.py dan query_tree.py

    @staticmethod
    def parse_query(query: str) -> ParsedQuery:
        pass

    @staticmethod
    def optimize_query(query: ParsedQuery) -> ParsedQuery:
        pass

    @staticmethod
    def get_cost(query: ParsedQuery) -> int:
        pass