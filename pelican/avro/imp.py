from datetime import datetime

import json
from fastavro import reader
from io import BytesIO
from pfb.base import handle_schema_field_b64, is_enum, b64_decode


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


def import_avro(spark, pfb_file, ddt, db_url, db_user, db_pass):
    start_time = datetime.now()
    print(start_time)

    properties = {"user": db_user, "password": db_pass, "driver": "org.postgresql.Driver"}

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
            .jdbc(url=db_url, table=ddt.get_node_label_by_table()[n], mode=mode, properties=properties)

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
