import json
from collections import defaultdict
from datetime import datetime


def create_node_dict(node_id, node_name, values, edges):
    inside = json.loads(values)

    node_dict = {
        "id": node_id,
        "name": node_name,
        "object": inside,
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


def export_avro(spark, pfb_file, dd_tables, traverse_order, case_ids, db_url, db_user, db_pass, root_node):
    pfb_file.open_mode = "a+b"

    start_time = datetime.now()
    print(start_time)

    node_label, edge_label = dd_tables

    db = spark.read.format("jdbc").options(
        url=db_url, user=db_user, password=db_pass, driver="org.postgresql.Driver"
    )

    it = defaultdict(list)

    for e, v in edge_label.items():
        it[v["src"]].append(e)

    table_logs = "{:<40}"
    current_ids = defaultdict(list)

    prev = root_node
    current_ids[prev] = case_ids
    node_edges = defaultdict(list)

    for k in traverse_order:
        v = it[k]
        for edge_table in v:
            dst_table_name = edge_label[edge_table]["dst"]
            src_table_name = edge_label[edge_table]["src"]
            edges = get_ids_from_table(db, edge_table, current_ids[dst_table_name], "dst_id")

            if not edges:
                print('[WARNING]' + table_logs.format(edge_table))
                continue

            edges = edges.rdd.map(
                lambda x: {
                    "src_id": x["src_id"],
                    "dst_id": x["dst_id"],
                    "dst_name": dst_table_name,
                }
            )
            print(table_logs.format(edge_table))

            for e in edges.toLocalIterator():
                node_edges[e["src_id"]].append(
                    {"dst_id": e["dst_id"], "dst_name": e["dst_name"]}
                )

            current_ids[src_table_name].extend(node_edges.keys())

        node_table = "node_" + k.replace("_", "")
        node_name = node_label[node_table]

        prev = k

        nodes = get_ids_from_table(db, node_table, current_ids[prev], "node_id")

        if not nodes:
            print(table_logs.format(node_table))
            continue

        nodes = nodes.rdd.map(
            lambda x: create_node_dict(
                x["node_id"], node_name, x["_props"], node_edges
            )
        )
        print(table_logs.format(node_table))

        pfb_file.write(nodes.toLocalIterator(), metadata=False)

    time_elapsed = datetime.now() - start_time
    print("Elapsed time: {}".format(time_elapsed))

    return
