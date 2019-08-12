from dictionaryutils import DataDictionary, dictionary


def init_dictionary(url):
    d = DataDictionary(url=url)
    dictionary.init(d)
    # the gdcdatamodel expects dictionary initiated on load, so this can't be
    # imported on module level
    from gdcdatamodel import models as md

    return d, md


def get_nodes(model):
    return model.Node.__subclasses__()


def get_edges(model):
    return model.Edge.__subclasses__()


def get_tables(model):
    nodes = get_nodes(model)
    node_tables = {node.__tablename__: str(node.label) for node in nodes}

    edges = get_edges(model)
    edge_tables = {
        edge.__tablename__: {
            "src": model.Node.get_subclass_named(edge.__src_class__).label,
            "dst": model.Node.get_subclass_named(edge.__dst_class__).label,
        }
        for edge in edges
    }

    return node_tables, edge_tables


def get_all_paths_bfs(model, node_name):
    queue = [node_name]

    visited = {}

    r = []

    while queue:
        s = queue.pop(0)

        node = model.Node.get_subclass(s).__name__
        edges = model.Edge._get_edges_with_dst(node)

        r.append(s)

        for i in [
            model.Node.get_subclass_named(e.__src_class__).get_label() for e in edges
        ]:
            if i not in visited:
                queue.append(i)
                visited[i] = True

    return r


def get_all_paths_dfs(model, node_name):
    stack, path = [node_name], []

    while stack:
        vertex = stack.pop()
        if vertex in path:
            continue
        path.append(vertex)

        node = model.Node.get_subclass(vertex).__name__
        edges = model.Edge._get_edges_with_dst(node)

        for neighbor in [
            model.Node.get_subclass_named(e.__src_class__).get_label() for e in edges
        ]:
            stack.append(neighbor)

    return path
