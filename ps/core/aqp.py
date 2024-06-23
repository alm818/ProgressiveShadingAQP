import sqlparse
from sqlparse import sql, tokens
from typing import List
from ps.util.debug import deb
from ps.core.predicate import Predicate

ALLOWED_KEYWORDS = {"SELECT":1, "FROM":1, "ON":0, "AS":0, "JOIN":0, "WHERE":1, "NOT":0, "AND":0, "OR":0, "GROUP BY":1}

class NameGraph:
    
    def __init__(self):
        self.edges = {}
        self.inversed_edges = {}

    def add_node(self, node):
        if node not in self.edges:
            self.edges[node] = set()
            self.inversed_edges[node] = set()
    
    def add_edge(self, start_node, end_node):
        assert(start_node in self.edges and end_node in self.edges)
        self.edges[start_node].add(end_node)
        self.inversed_edges[end_node].add(start_node)

    def get_size(self):
        return len(self.edges)

    def get_leaves(self):
        return [node for node in self.edges if len(self.edges[node]) == 0]

    def get_roots(self):
        return [node for node in self.edges if len(self.inversed_edges[node]) == 0]

    def get_topological_order(self):
        return [node for (_, node) in sorted([(len(edges), node) for node, edges in self.inversed_edges.items()])]

def get_identifiers(token):
    match type(token):
        case sql.IdentifierList:
            return [child.get_real_name() for child in token if isinstance(child, sql.Identifier)]
        case sql.Identifier:
            return [token.get_real_name()]
class AQP:

    select_columns: List[str]
    from_tables: NameGraph
    groupby_columns: List[str]
    where_predicate: Predicate

    def __init__(self, query):
        self.select_columns = []
        self.from_tables = None
        self.groupby_columns = []
        self.where_predicate = None

        self.query = query
        self.ast = sqlparse.parse(query)
        self.is_valid = True
        if self.ast:
            self.ast = self.ast[0]
        else:
            self.is_valid = "Query is not valid SQL"
        self._validate()
        if self.is_valid:
            next_idx = -1
            while True:
                next_idx, next_token = self.ast.token_next(next_idx)
                if next_idx is None:
                    break
                if isinstance(next_token, sql.Token) and (next_token.ttype == tokens.Keyword or next_token.ttype == tokens.Keyword.DML):
                    keyword = next_token.value.upper()
                    next_idx, next_token = self.ast.token_next(next_idx)
                    match keyword:
                        case "SELECT":
                            # Get select_columns
                            self.select_columns = get_identifiers(next_token)
                        case "FROM":
                            # Get from_tables
                            self.from_tables = NameGraph()
                            if isinstance(next_token, sql.Identifier):
                                self.from_tables.add_node(next_token.get_real_name())
                        case "GROUP BY":
                            self.groupby_columns = get_identifiers(next_token)
                elif isinstance(next_token, sql.Where):
                    self.where_predicate = Predicate(next_token)

    def _validate(self):
        pass

    def print(self, node=None, indent=-1):
        if node is None:
            self.print(self.ast)
        else:
            if isinstance(node, sql.TokenList):
                print("\t"*(indent+1), type(node))
                for child in node:
                    self.print(child, indent+1)
            else:
                print("\t"*indent, type(node), node.ttype, node)