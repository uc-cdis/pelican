import json
from collections import defaultdict
from datetime import datetime


def create_node_dict(node_id, node_name, values, edges):
    node_dict = {
        "id": node_id,
        "name": node_name,
        "object": values,
        "relations": edges[node_id] if node_id in edges else [],
    }

    return node_dict


def split_by_n(input_list, n=1000):
    return [input_list[x:x + n] for x in range(0, len(input_list), n)]


def get_ids_from_table(db, table, ids, id_column):
    data = None

    for ids_chunk in split_by_n(ids):
        current_chunk_data = db \
            .option("query", "SELECT * FROM {} WHERE {} IN ('{}')".format(table, id_column, "','".join(ids_chunk))) \
            .load()

        if data:
            data = data.union(current_chunk_data)
        else:
            data = current_chunk_data

    return data if data and data.first() else None


def export_avro(db, pfb_file, ddt, case_ids, root_node):
    pfb_file.open_mode = "a+b"

    start_time = datetime.now()
    print(start_time)

    it = ddt.get_edges_by_node()

    table_logs = "{:<40}"
    current_ids = defaultdict(list)

    current_ids[root_node] = case_ids
    node_edges = defaultdict(list)

    for way, node_name in ddt.full_traverse_path(root_node):
        v = it[node_name]
        for edge_table in v:
            if way:
                src, dst = "src", "dst"
            else:
                src, dst = "dst", "src"

            src_table_name = ddt.get_edge_labels_by_table()[edge_table][src]
            dst_table_name = ddt.get_edge_labels_by_table()[edge_table][dst]

            src += "_id"
            dst += "_id"

            edges = get_ids_from_table(db, edge_table, current_ids[dst_table_name], dst)

            if not edges:
                print('[WARNING]' + table_logs.format(edge_table))
                continue

            edges = edges.rdd.map(
                lambda x: {
                    "src_id": x["src_id"],
                    "dst_id": x["dst_id"],
                }
            )
            print(table_logs.format(edge_table))

            for e in edges.toLocalIterator():
                node_edges[e[src]].append(
                    {"dst_id": e[dst], "dst_name": dst_table_name}
                )

            current_ids[src_table_name].extend(node_edges.keys())

        node_table = ddt.get_node_table_by_label()[node_name]

        nodes = get_ids_from_table(db, node_table, current_ids[node_name], "node_id")

        if not nodes:
            print('[WARNING]' + table_logs.format(node_table))
            continue

        nodes = nodes.rdd.map(
            lambda x: create_node_dict(
                x["node_id"], node_name, json.loads(x["_props"]), node_edges
            )
        )
        print(table_logs.format(node_table))

        pfb_file.write(nodes.toLocalIterator(), metadata=False)

    time_elapsed = datetime.now() - start_time
    print("Elapsed time: {}".format(time_elapsed))

    return
