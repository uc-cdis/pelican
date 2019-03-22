import datetime
import json
from io import BytesIO

from fastavro import reader
from flask import current_app as app
from pyspark.sql import Row

from utils.avro import create_avro_from
from utils.dictionary import *
from utils.encoding import *


def is_enum(field):
    if isinstance(field['type'], list):
        if 'type' in field['type'][0]:
            is_enum = field['type'][0]['type'] == 'enum'
        else:
            is_enum = False
    elif 'type' in field['type'] and field['type']['type'] == 'enum':
        is_enum = True
    else:
        is_enum = False
    return is_enum


def import_avro(spark, model, filename, url, mode, properties):
    avro = app.sc.binaryFiles(filename)

    parsed_schema = avro.map(lambda x: reader(BytesIO(x[1])).writer_schema).first()

    avro =  avro \
        .flatMap(lambda x: reader(BytesIO(x[1])))

    # get all node types from the Avro file
    node_types = avro.map(lambda v: v['name']).distinct().collect()

    def sql_format_node(node, enums):
        encoded_node = node['object']

        for enum in enums:
            encoded_node[enum] = base64.b64decode(str(encoded_node[enum]) + '===')

        sql_node = {
            'created': datetime.datetime.now(),
            'acl': json.dumps({}),
            '_sysan': json.dumps({}),
            '_props': json.dumps(encoded_node),
            'node_id': node['id']
        }
        return Row(**sql_node)

    # import node of type into corresponding Postgres node table
    for t in node_types:
        if t == 'metadata':
            continue
        print('node: {}'.format(t))
        node_tablename = get_node_table_name(model, t)

        # store the list of enums for current node type for decoding
        node_schema = filter(lambda x: x['name'] == t, parsed_schema['fields'][2]['type'])[0]
        enums = []
        for v in node_schema['fields']:
            if is_enum(v):
                enums.append(v['name'])

        avro \
            .filter(lambda node: node['name'] == t) \
            .map(lambda v: sql_format_node(v, enums)) \
            .toDF() \
            .write \
            .jdbc(url=url, table=node_tablename, mode=mode, properties=properties)

    r = [get_related_nodes_for(model, v) for v in get_nodes(model)]

    related_nodes_for = {}
    for i in r:
        related_nodes_for.update(i)

    edges_rdd = avro \
        .flatMap(lambda v:
                 [{'src_id': v['id'],
                   'dst_id': r['dst_id'],
                   'src': r['dst_name'],
                   'dst': v['name'],
                   'table': related_nodes_for[(r['dst_name'], v['name'])]}
                  for r in v['relations']]
                 )

    edges_tables = edges_rdd.map(lambda v: v['table']).distinct().collect()

    def sql_format_edge(edge):
        return Row(**{
            'created': datetime.datetime.now(),
            'acl': json.dumps({}),
            '_sysan': json.dumps({}),
            '_props': json.dumps({}),
            'src_id': edge['src_id'],
            'dst_id': edge['dst_id']
        })

    # import edges into corresponding Postgres edge table
    for e in edges_tables:
        print('edge: {}'.format(e))
        edges_rdd \
            .filter(lambda edge: edge['table'] == e) \
            .map(sql_format_edge) \
            .toDF() \
            .write \
            .jdbc(url=url, table=e, mode=mode, properties=properties)
