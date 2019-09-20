import json
from collections import defaultdict
from datetime import datetime
from io import BytesIO

from fastavro import reader
from pfb.base import handle_schema_field_b64, is_enum, b64_decode


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


def export_pfb_job(db, pfb_file, ddt, case_ids, root_node):
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


def convert_to_node(x, is_base64):
    obj = x["object"]
    to_update = {}
    for name, value in obj.iteritems():
        if value and is_base64[x["name"]][name]:
            to_update[name] = b64_decode(value)

    obj.update(to_update)

    r = {"created": datetime.now(),
         "acl": json.dumps({}),
         "_sysan": json.dumps({}),
         "_props": json.dumps(obj),
         "node_id": x["id"]}

    return r


def convert_to_edge(x, edge_tables):
    return [(edge_tables[(x["name"], i["dst_name"])], {"created": datetime.now(),
                                                       "acl": json.dumps({}),
                                                       "_sysan": json.dumps({}),
                                                       "_props": json.dumps({}),
                                                       "src_id": x["id"],
                                                       "dst_id": i["dst_id"]}) for i in x["relations"]]


def import_pfb_job(spark, pfb_file, ddt, db_url, db_user, db_pass):
    start_time = datetime.now()
    print(start_time)

    properties = {"user": db_user, "password": db_pass, "driver": "org.postgresql.Driver", "stringtype": "unspecified"}

    with open(pfb_file) as f:
        avro_reader = reader(f)
        schema = avro_reader.writer_schema

    s = []
    for f in schema["fields"]:
        if f["name"] == "object":
            it = iter(f["type"])
            # skip metadata
            next(it)
            for node in it:
                s.append(node)
                for field in node["fields"]:
                    handle_schema_field_b64(field, encode=False)

    _is_base64 = {}

    for node in s:
        _is_base64[node["name"]] = fields = {}
        for field in node["fields"]:
            fields[field["name"]] = is_enum(field["type"])

    rdd = spark.sparkContext.binaryFiles(pfb_file) \
        .flatMap(lambda args: reader(BytesIO(args[1])))

    mode = "append"

    distinct_nodes = rdd \
        .map(lambda x: x['name']) \
        .distinct() \
        .filter(lambda x: x != "Metadata") \
        .collect()

    for n in distinct_nodes:
        rdd \
            .filter(lambda x: x["name"] == n) \
            .map(lambda x: convert_to_node(x, _is_base64)) \
            .toDF() \
            .write \
            .jdbc(url=db_url, table=ddt.get_node_table_by_label()[n], mode=mode, properties=properties)

    distinct_edges = rdd \
        .flatMap(lambda x: convert_to_edge(x, ddt.get_edge_table_by_labels())) \
        .map(lambda x: x[0]) \
        .distinct() \
        .collect()

    for e in distinct_edges:
        rdd \
            .flatMap(lambda x: convert_to_edge(x, ddt.get_edge_table_by_labels())) \
            .filter(lambda x: x[0] == e) \
            .toDF() \
            .write \
            .jdbc(url=db_url, table=e, mode=mode, properties=properties)

    time_elapsed = datetime.now() - start_time
    print("Elapsed time: {}".format(time_elapsed))

    return
