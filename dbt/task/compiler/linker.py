import sqlparse
import networkx as nx

class Linker(object):
    def __init__(self, graph=None):
        if graph is None:
            self.graph = nx.DiGraph()
        else:
            self.graph = graph

        self.node_sql_map = {}

    def serialized(self):
        return "\n".join(nx.generate_adjlist(self.graph))

    @classmethod
    def deserialize(self, serialized_file):
        graph = nx.read_adjlist(serialized_file, create_using=nx.DiGraph())
        return Linker(graph)

    def extract_name_and_deps(self, stmt):
        table_def = stmt.token_next_by_instance(0, sqlparse.sql.Identifier)
        schema, tbl_or_view =  table_def.get_parent_name(), table_def.get_real_name()
        if schema is None or tbl_or_view is None:
            print "SCHEMA: ", schema
            print "TBL/VIEWL ", tbl_or_view
            print "DEF: ", table_def
            raise RuntimeError('schema or view not defined?')

        definition = table_def.token_next_by_instance(0, sqlparse.sql.Parenthesis)

        node = (schema, tbl_or_view)

        i = 0
        token = definition.token_next_by_instance(0, sqlparse.sql.Identifier)
        while token is not None:
            new_node = (token.get_parent_name(), token.get_real_name())
            # not great -- our parser doesn't differentiate between SELECTed fields and tables
            if None not in new_node:
                self.graph.add_node(new_node)
                self.graph.add_edge(node, new_node)

            i = definition.token_index(token) + 1
            token = definition.token_next_by_instance(i, sqlparse.sql.Identifier)

        return node

    def as_dependency_list(self):
        order = nx.topological_sort(self.graph, reverse=True)
        for node in order:
            if node in self.node_sql_map: # TODO : check against db??? or what?
                yield (node, self.node_sql_map[node])
            else:
                print "Skipping {}".format(node)

    def register(self, node, sql):
        if node in self.node_sql_map:
            raise RuntimeError("multiple declarations of node: {}".format(node))
        self.node_sql_map[node] = sql

    def link(self, sql):
        sql = sql.strip()
        for statement in sqlparse.parse(sql):
            if statement.get_type() == 'CREATE':
                print "DEBUG: ERROR: Use CREATE OR REPLACE instead of CREATE!"
            elif statement.get_type() == 'CREATE OR REPLACE':
                node = self.extract_name_and_deps(statement)
                self.register(node, sql)

