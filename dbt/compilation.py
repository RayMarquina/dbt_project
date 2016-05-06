
import networkx as nx

class Linker(object):
    def __init__(self):
        self.graph = nx.DiGraph()

    def as_dependency_list(self):
        return nx.topological_sort(self.graph, reverse=True)

    def dependency(self, node1, node2):
        "indicate that node1 depends on node2"
        self.graph.add_node(node1)
        self.graph.add_node(node2)
        self.graph.add_edge(node1, node2)

    def add_node(self, node):
        self.graph.add_node(node)

    def write_graph(self, outfile):
        nx.write_yaml(self.graph, outfile)

    def read_graph(self, infile):
        self.graph = nx.read_yaml(infile)

