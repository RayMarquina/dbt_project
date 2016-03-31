import sqlparse
import networkx as nx

class Relation(object):
    def __init__(self, schema, name):
        self.schema = schema
        self.name = name

    def valid(self):
        return None not in (self.schema, self.name)

    @property
    def val(self):
        return "{}.{}".format(self.schema, self.name)

    def __repr__(self):
        return self.val()

    def __str__(self):
        return self.val()

class Linker(object):
    def __init__(self, graph=None):
        if graph is None:
            self.graph = nx.DiGraph()
        else:
            self.graph = graph

        self.node_sql_map = {}

    def extract_name_and_deps(self, stmt):
        table_def = stmt.token_next_by_instance(0, sqlparse.sql.Identifier)
        schema, tbl_or_view =  table_def.get_parent_name(), table_def.get_real_name()
        if schema is None or tbl_or_view is None:
            raise RuntimeError('schema or view not defined?')

        definition = table_def.token_next_by_instance(0, sqlparse.sql.Parenthesis)

        definition_node = Relation(schema, tbl_or_view)

        local_defs = set()
        new_nodes = set()

        def extract_deps(stmt):
            token = stmt.token_first()
            while token is not None:
                excluded_types = [sqlparse.sql.Function] # don't dive into window functions
                if type(token) not in excluded_types and token.is_group():
                    # this is a thing that has a name -- note that!
                    local_defs.add(token.get_name())
                    # recurse into the group
                    extract_deps(token)

                if type(token) == sqlparse.sql.Identifier:
                    new_node = Relation(token.get_parent_name(), token.get_real_name())

                    if new_node.valid():
                        new_nodes.add(new_node) # don't add edges yet!

                index = stmt.token_index(token)
                token = stmt.token_next(index)

        extract_deps(definition)

        # only add nodes which don't reference locally defined constructs
        for new_node in new_nodes:
            if new_node.schema not in local_defs:
                self.graph.add_node(new_node.val)
                self.graph.add_edge(definition_node.val, new_node.val)

        return definition_node.val

    def as_dependency_list(self):
        order = nx.topological_sort(self.graph, reverse=True)
        for node in order:
            if node in self.node_sql_map: # TODO :
                yield (node, self.node_sql_map[node])
            else:
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
