import itertools
from collections import defaultdict

from dictionaryutils import DataDictionary, dictionary


def init_dictionary(url):
    d = DataDictionary(url=url)
    dictionary.init(d)
    # the gdcdatamodel expects dictionary initiated on load, so this can't be
    # imported on module level
    from gdcdatamodel import models as md

    return d, md


class DataDictionaryTraversal:
    def __init__(self, model):
        self.model = model

    def get_nodes(self):
        return self.model.Node.__subclasses__()

    def get_edges(self):
        return self.model.Edge.__subclasses__()

    def get_node_table_by_label(self):
        nodes = self.get_nodes()
        node_tables = {str(node.label): node.__tablename__ for node in nodes}
        return node_tables

    def get_node_label_by_table(self):
        nodes = self.get_nodes()
        node_tables = {node.__tablename__: str(node.label) for node in nodes}
        return node_tables

    def get_edge_table_by_labels(self):
        edges = self.get_edges()
        edge_tables = {
            (self.model.Node.get_subclass_named(edge.__src_class__).label,
             self.model.Node.get_subclass_named(edge.__dst_class__).label): edge.__tablename__ for edge in edges
        }
        return edge_tables

    def get_edge_labels_by_table(self):
        edges = self.get_edges()
        edge_tables = {
            edge.__tablename__: {
                "src": self.model.Node.get_subclass_named(edge.__src_class__).label,
                "dst": self.model.Node.get_subclass_named(edge.__dst_class__).label,
            }
            for edge in edges
        }
        return edge_tables

    def get_edges_by_node(self):
        edges = self.get_edges()
        it = defaultdict(list)

        for edge in edges:
            it[self.model.Node.get_subclass_named(edge.__src_class__).label].append(edge.__tablename__)

        return it

    def _get_bfs(self, node_name):
        queue = [node_name]

        visited = {}

        r = []

        while queue:
            s = queue.pop(0)

            node = self.model.Node.get_subclass(s).__name__
            edges = self.model.Edge._get_edges_with_dst(node)

            r.append(s)

            for i in [
                self.model.Node.get_subclass_named(e.__src_class__).get_label() for e in edges
            ]:
                if i not in visited:
                    queue.append(i)
                    visited[i] = True

        return r

    def _get_dfs(self, node_name, source_edges, target_class):
        stack, path = [node_name], []

        while stack:
            vertex = stack.pop()
            if vertex in path:
                continue
            path.append(vertex)

            node = self.model.Node.get_subclass(vertex).__name__
            edges = getattr(self.model.Edge, source_edges)(node)

            for neighbor in [
                self.model.Node.get_subclass_named(getattr(e, target_class)).get_label() for e in edges
            ]:
                stack.append(neighbor)

        return path

    def _topology_order(self, node_name, source_edges, target_class):
        stack, path = [node_name], []

        while stack:
            vertex = stack[-1]

            node = self.model.Node.get_subclass(vertex).__name__
            edges = getattr(self.model.Edge, source_edges)(node)

            children = [self.model.Node.get_subclass_named(getattr(e, target_class)).get_label() for e in edges]

            visited_children = [child for child in children if child not in path]

            if not visited_children:
                path.insert(0, vertex)
                stack.pop()
            else:
                stack.append(visited_children[0])

        return path

    def get_upward_path(self, node_name):
        return self._topology_order(node_name, "_get_edges_with_src", "__dst_class__")

    def get_downward_path(self, node_name):
        return self._topology_order(node_name, "_get_edges_with_dst", "__src_class__")

    def full_traverse_path(self, node_name, extra_nodes=None, include_upward=False):
        if include_upward:
            upward_path = list(zip(itertools.repeat(False), self.get_upward_path(node_name)))
            downward_path = list(zip(itertools.repeat(True), self.get_downward_path(node_name)))[1:]
            if extra_nodes:
                path = upward_path + list(zip(itertools.repeat(True), extra_nodes)) + downward_path
            else:
                path = upward_path + downward_path
        else:
            downward_path = list(zip(itertools.repeat(True), self.get_downward_path(node_name)))
            path = downward_path

        return path
