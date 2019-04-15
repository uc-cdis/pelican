import datetime
import json
from collections import defaultdict
from io import BytesIO

from fastavro import reader
from fastavro import writer
from flask import Flask, Blueprint, send_file, after_this_request, request, jsonify, request
from flask import current_app as app
from flask_restful import Resource, Api
from pyspark.sql import SparkSession, Row
from werkzeug.utils import secure_filename

from config import config
from spark import *
from utils.avro import create_avro_from
from utils.dictionary import *


from avro_utils.avro_schema import AvroSchema

BASE_DIR = os.path.abspath(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
)


def dsn_string(host, user, password, db, port=5432):
    dsn = 'postgresql://{user}:{password}@{host}:{port}/{db}'.format(user=user, password=password, host=host, port=port,
                                                                     db=db)
    return dsn


class Schema(Resource):
    def get(self):
        return app.schema


class Status(Resource):
    def get(self):
        response = jsonify({'status': 'ok'})
        response.status_code = 200
        return response


class Import(Resource):
    def put(self):
        input_file = request.files['file']

        if input_file:
            url = app.config['DB_URL']
            mode = 'append'
            properties = {
                "user": app.config['DB_USER'],
                "password": app.config['DB_PASS'],
                'driver': 'org.postgresql.Driver',
                "stringtype": "unspecified"
            }

            filename = secure_filename(input_file.filename)
            filename = os.path.join(BASE_DIR, 'pelican', 'upload', filename)
            input_file.save(filename)

            spark = SparkSession.builder.getOrCreate()
            import_avro(spark, app.model, 'file://' + filename, url, mode, properties)

            response = jsonify(dict(result='true'))
            response.status_code = 200
            return response
        else:
            response = jsonify(dict())
            response.status_code = 400
            return {'False'}


class Export(Resource):
    def get(self):
        if request.get_json():
            print(request.get_json()['case'])

        if False:
            spark = SparkSession.builder.getOrCreate()
            avro_filename = export_avro(spark, app.schema, app.metadata, app.dd_tables, app.config['DB_URL'], app.config['DB_USER'], app.config['DB_PASS'])

            @after_this_request
            def remove_temporary_file(response):
                # remove the temporary file after response
                os.remove(avro_filename)
                return response

            return send_file(avro_filename, as_attachment=True, attachment_filename='full_dump.avro')


def create_app(env, sc=None):
    app = Flask(__name__)
    app.config.from_object(config[env])

    api_bp = Blueprint('api', __name__)
    api = Api(api_bp)

    api.add_resource(Schema, '/schema')
    api.add_resource(Status, '/_status')
    api.add_resource(Import, '/import')
    api.add_resource(Export, '/export')
    app.register_blueprint(api_bp, url_prefix="/api/v1")

    dictionary_url = app.config['DICTIONARY_URL']
    dictionary, model = init_dictionary(url=dictionary_url)
    app.dictionary = dictionary
    app.model = model
    avro_schema = AvroSchema.from_dictionary(dictionary.schema)
    schema = avro_schema.avro_schema
    metadata = avro_schema.get_ontology_references()
    app.schema = schema
    app.metadata = metadata
    node_tables, edge_tables = get_tables(model)
    app.dd_tables = (node_tables, edge_tables)

    if not sc:
        app.sc = init_spark_context()
    else:
        app.sc = sc

    return app
