import datetime
import json
from collections import defaultdict
from io import BytesIO

from fastavro import reader
from fastavro import writer
from flask import Flask, Blueprint, send_file, after_this_request, request
from flask import current_app as app
from flask_restful import Resource, Api
from pyspark.sql import SparkSession, Row
from werkzeug.utils import secure_filename

from config import config
from spark import *
from utils.avro import create_avro_from
from utils.dictionary import *
from utils.encoding import *

BASE_DIR = os.path.abspath(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
)


def dsn_string(host, user, password, db, port=5432):
    dsn = 'postgresql://{user}:{password}@{host}:{port}/{db}'.format(user=user, password=password, host=host, port=port,
                                                                     db=db)
    return dsn


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


class Schema(Resource):
    def get(self):
        return app.schema


class Import(Resource):
    def put(self):
        input_file = request.files['file']

        spark = SparkSession.builder.getOrCreate()

        if input_file:
            filename = secure_filename(input_file.filename)
            input_file.save(os.path.join('/Users/andrewprokhorenkov/CTDS/pelican/upload', filename))

            avro = app.sc.binaryFiles(
                "file://" + os.path.join('/Users/andrewprokhorenkov/CTDS/pelican/upload', filename)) \
                .flatMap(lambda x: reader(BytesIO(x[1])))

            # get all node types from the Avro file
            node_types = avro.map(lambda v: v['name']).distinct().collect()

            url = "jdbc:postgresql://localhost/metadata_db"
            properties = {
                "user": "postgres",
                "password": "postgres",
                'driver': 'org.postgresql.Driver',
                "stringtype": "unspecified"
            }
            mode = 'append'

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

            _, parsed_schema = create_avro_from('dd_schema.avro')

            for t in node_types:
                if t == 'metadata':
                    continue
                print('node: {}'.format(t))
                node_tablename = get_node_table_name(app.model, t)

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

            r = [get_related_nodes_for(app.model, v) for v in get_nodes(app.model)]

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

            def sql_format_edge(edge):
                return Row(**{
                    'created': datetime.datetime.now(),
                    'acl': json.dumps({}),
                    '_sysan': json.dumps({}),
                    '_props': json.dumps({}),
                    'src_id': edge['src_id'],
                    'dst_id': edge['dst_id']
                })

            edges_tables = edges_rdd.map(lambda v: v['table']).distinct().collect()

            for e in edges_tables:
                print('edge: {}'.format(e))
                edges_rdd \
                    .filter(lambda edge: edge['table'] == e) \
                    .map(sql_format_edge) \
                    .toDF() \
                    .write \
                    .jdbc(url=url, table=e, mode=mode, properties=properties)

            return {'result': 'True'}
        else:
            # return error
            return {'False'}


class ExportProject(Resource):
    def get(self):
        node_label, edge_label = app.dd_tables
        spark = SparkSession.builder.getOrCreate()

        db = spark.read.format('jdbc'). \
            options(
            url='jdbc:postgresql://localhost/metadata_db',
            user='postgres',
            password='postgres',
            driver='org.postgresql.Driver')

        all_tables = db.options(dbtable='information_schema.tables').load().select('table_name')

        it = defaultdict(list)

        edge_tables = [t['table_name'] for t in all_tables.toLocalIterator() if t['table_name'].startswith('edge_')]

        for e, v in edge_label.items():
            if e in edge_tables:
                it[v['src']].append(e)

        table_logs = '{:>40} = {:<10} [{:<12}]'
        total = 0

        avro_filename, parsed_schema = create_avro_from('dd_schema.avro')

        for k, v in it.items():
            node_edges = defaultdict(list)
            for edge_table in v:
                dst_table_name = edge_label[edge_table]['dst']
                edges = db.options(dbtable=edge_table).load().rdd.map(
                    lambda x: {'src_id': x['src_id'], 'dst_id': x['dst_id'],
                               'dst_name': dst_table_name})

                total += edges.count()
                print(table_logs.format(edge_table, edges.count(), total))

                for e in edges.toLocalIterator():
                    node_edges[e['src_id']].append({'dst_id': e['dst_id'], 'dst_name': e['dst_name']})

            node_table = 'node_' + k.replace('_', '')
            node_name = node_label[node_table]

            node_schema = filter(lambda x: x['name'] == node_name, parsed_schema['fields'][2]['type'])[0]

            def create_node_dict(node_id, node_name, values, node_schema, edges):
                inside = json.loads(values)

                vals = {}

                for k, v in inside.iteritems():
                    is_unicode = type(v) == unicode
                    current_node_schema = filter(lambda x: x['name'] == k, node_schema['fields'])

                    if len(current_node_schema) > 0:
                        current_node_schema = current_node_schema[0]
                    else:
                        print('{} is not in the schema for {}'.format(k, node_name))
                        continue

                    if isinstance(current_node_schema['type'], list):
                        if 'type' in current_node_schema['type'][0]:
                            is_enum = current_node_schema['type'][0]['type'] == 'enum'
                        else:
                            is_enum = False
                    elif 'type' in current_node_schema['type'] and current_node_schema['type']['type'] == 'enum':
                        is_enum = True
                    else:
                        is_enum = False

                    if is_unicode and not is_enum:
                        val = str(v)
                    elif is_enum:
                        val = base64.b64encode(str(v)).rstrip("=")
                    else:
                        val = v

                    vals[str(k)] = val

                node_dict = {
                    'id': node_id,
                    'name': node_name,
                    'object': (node_name, vals),
                    'relations': edges[node_id] if node_id in edges else []
                }

                return node_dict

            nodes = db.options(dbtable=node_table).load().rdd.map(
                lambda x: create_node_dict(x['node_id'], node_name, x['_props'], node_schema, node_edges))
            total += nodes.count()
            print(table_logs.format(node_table, nodes.count(), total))

            with open(avro_filename, 'a+b') as output_file:
                writer(output_file, parsed_schema, nodes.toLocalIterator())

        @after_this_request
        def remove_temporary_file(response):
            # remove the temporary file after response
            os.remove(avro_filename)
            return response

        return send_file(avro_filename, as_attachment=True, attachment_filename='full_dump.avro')


class ExportId(Resource):
    def get(self, node_id):
        print(node_id)
        raise NotImplementedError


def create_app(env, sc=None):
    app = Flask(__name__)
    app.config.from_object(config[env])

    api_bp = Blueprint('api', __name__)
    api = Api(api_bp)

    api.add_resource(Schema, '/schema')
    api.add_resource(Import, '/import')
    api.add_resource(ExportProject, '/export')
    api.add_resource(ExportId, '/export/<node_id>')
    app.register_blueprint(api_bp, url_prefix="/api/v1")

    dictionary_url = app.config['DICTIONARY_URL']
    _, model = init_dictionary(url=dictionary_url)
    node_tables, edge_tables = get_tables(model)
    app.model = model
    app.dd_tables = (node_tables, edge_tables)

    if not sc:
        app.sc = init_spark_context()
    else:
        app.sc = sc

    return app
