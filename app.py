import json
import os
import sys
from datetime import datetime

from avro_utils.avro_schema import AvroSchema
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pelican.avro.export import export_avro
from pelican.dictionary import get_all_paths, init_dictionary, get_tables
from pelican.graphql.guppy_gql import GuppyGQL
from pelican.s3 import s3upload_file

if __name__ == "__main__":
    node = os.environ["ROOT_NODE"]
    access_token = os.environ["ACCESS_TOKEN"]
    hostname = os.environ["GEN3_HOSTNAME"]
    input_data = os.environ["INPUT_DATA"]

    gql = GuppyGQL(node=node, url="https://{}".format(hostname), access_token=access_token)
    case_ids = gql.execute(filters=input_data)

    sys.stderr.write(str(case_ids))

    DICTIONARY_URL = os.environ["DICTIONARY_URL"]

    with open("/peregrine-creds.json") as pelican_creds_file:
        peregrine_creds = json.load(pelican_creds_file)

    DB_URL = "jdbc:postgresql://{}/{}".format(
        peregrine_creds["db_host"], peregrine_creds["db_database"]
    )
    DB_USER = peregrine_creds["db_username"]
    DB_PASS = peregrine_creds["db_password"]

    dictionary_url = DICTIONARY_URL
    dictionary, model = init_dictionary(url=dictionary_url)

    avro_schema = AvroSchema.from_dictionary(dictionary.schema)
    schema = avro_schema.avro_schema
    metadata = avro_schema.get_ontology_references()

    node_tables, edge_tables = get_tables(model)
    dd_tables = (node_tables, edge_tables)

    conf = (
        SparkConf()
        .set("spark.jars", os.environ["POSTGRES_JAR_PATH"])
        .set("spark.driver.memory", "10g")
        .set("spark.executor.memory", "10g")
        .setAppName("pelican")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    traverse_order = get_all_paths(model, node)

    avro_filename = export_avro(
        spark,
        schema,
        metadata,
        dd_tables,
        traverse_order,
        case_ids,
        DB_URL,
        DB_USER,
        DB_PASS,
        node
    )

    with open("/pelican-creds.json") as pelican_creds_file:
        pelican_creds = json.load(pelican_creds_file)

    s3file = s3upload_file(
        pelican_creds["manifest_bucket_name"],
        "{}.avro".format(datetime.now().strftime('export_%Y-%m-%dT%H:%M:%S')),
        pelican_creds["aws_access_key_id"],
        pelican_creds["aws_secret_access_key"],
        avro_filename
    )

    print("[out] {}".format(s3file))
