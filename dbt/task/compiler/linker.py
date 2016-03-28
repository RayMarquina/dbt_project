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

        definition_node = (schema, tbl_or_view)

        local_defs = set()

        def extract_deps(stmt):
            token = stmt.token_first()
            while token is not None:
                if type(token) != sqlparse.sql.IdentifierList and token.is_group():
                    local_defs.add(token.get_name())
                    extract_deps(token)

                if type(token) == sqlparse.sql.Identifier and token.get_parent_name() not in local_defs:
                    new_node = (token.get_parent_name(), token.get_real_name())
                    if None not in new_node:
                        self.graph.add_node(new_node)
                        self.graph.add_edge(definition_node, new_node)

                index = stmt.token_index(token)
                token = stmt.token_next(index)

        extract_deps(definition)
        return definition_node

    def as_dependency_list(self):
        order = nx.topological_sort(self.graph, reverse=True)
        for node in order:
            #print "{}.{}".format(node[0], node[1])
            if node in self.node_sql_map: # TODO : check against db??? or what?
                #for (schema, tbl_or_view) in self.graph[node]:
                #    print "   {}.{}".format(schema, tbl_or_view)
                yield (node, self.node_sql_map[node])
            else:
                #print "Skipping {}".format(node)
                pass

    def register(self, node, sql):
        if node in self.node_sql_map:
            raise RuntimeError("multiple declarations of node: {}".format(node))
        self.node_sql_map[node] = sql

    def link(self, sql):
        sql = sql.strip()
        for statement in sqlparse.parse(sql):
            if statement.get_type().startswith('CREATE'):
                node = self.extract_name_and_deps(statement)
                self.register(node, sql)
            else:
                print "Ignoring {}".format(sql[0:100].replace('\n', ' '))

