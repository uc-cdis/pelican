from dictionaryutils import DataDictionary, dictionary


def init_dictionary(url):
    d = DataDictionary(url=url)
    dictionary.init(d)
    # the gdcdatamodel expects dictionary initiated on load, so this can't be
    # imported on module level
    from gdcdatamodel import models as md
    return d, md


def get_related_nodes_for(model, node):
    node_name = node.__name__
    edges = model.Edge._get_edges_with_dst(node_name)
    related_nodes = {(node.get_label(), model.Node.get_subclass_named(e.__src_class__).get_label()): e.__tablename__
                     for e in edges}
    return related_nodes


def get_nodes(model):
    return model.Node.__subclasses__()


def get_edges(model):
    return model.Edge.__subclasses__()


def get_tables(model):
    nodes = get_nodes(model)
    node_tables = {node.__tablename__: str(node.label) for node in nodes}

    edges = get_edges(model)
    edge_tables = {edge.__tablename__: {'src': model.Node.get_subclass_named(edge.__src_class__).label,
                                        'dst': model.Node.get_subclass_named(edge.__dst_class__).label}
                   for edge in edges}

    return node_tables, edge_tables


def get_edge_table(models, node_label, edge_name):
    '''
    :param models: the model which node and edge belong to
    :param node_label: the label of a node
    :param edge_name: the back_ref label of an edge
    :return: (label of the source node, table name of edge specified by edge_name)
    '''
    node = models.Node.get_subclass(node_label)
    edge = getattr(node, edge_name)
    parent_label = get_node_label(models, edge.target_class.__src_class__)
    if node_label == parent_label:
        parent_label = get_node_label(models, edge.target_class.__dst_class__)
    return parent_label, edge.target_class.__tablename__


def get_child_table(models, node_name, edge_name):
    """
    Return the a table name indicated by node's name and edge name. Work as bidirectional link
    :param models: considering model
    :param node_name: known node name
    :param edge_name: link from/to node_name
    :return:
    """
    node = models.Node.get_subclass(node_name)
    edge = getattr(node, edge_name)
    src_class = edge.target_class.__src_class__
    src_label = get_node_label(models, src_class)
    if src_label != node_name:
        return models.Node.get_subclass_named(src_class).__tablename__, True
    return models.Node.get_subclass_named(edge.target_class.__dst_class__).__tablename__, False


def get_all_children_of_node(models, class_name):
    name = models.Node.get_subclass_named(class_name).__name__
    edges_in = models.Edge._get_edges_with_dst(name)
    return edges_in


def get_parent_name(models, node_name, edge_name):
    node = models.Node.get_subclass(node_name)
    edge = getattr(node, edge_name)
    return models.Node.get_subclass_named(edge.target_class.__dst_class__).__name__


def get_parent_label(models, node_name, edge_name):
    node = models.Node.get_subclass(node_name)
    edge = getattr(node, edge_name)
    return models.Node.get_subclass_named(edge.target_class.__dst_class__).get_label()


def get_node_label(models, node_name):
    node = models.Node.get_subclass_named(node_name)
    return node.get_label()


def get_node_table_name(models, node_name):
    node = models.Node.get_subclass(node_name)
    return node.__tablename__


def get_properties_types(models, node_name):
    node = models.Node.get_subclass(node_name)
    return node.__pg_properties__
