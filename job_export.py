import sys
from datetime import datetime

import json
import os
import tempfile
from pfb.importers.gen3dict import _from_dict
from pfb.reader import PFBReader
from pfb.writer import PFBWriter
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pelican.avro.export import export_avro
from pelican.dictionary import get_all_paths_dfs, init_dictionary, get_tables
from pelican.graphql.guppy_gql import GuppyGQL
from pelican.s3 import s3upload_file

if __name__ == "__main__":
    node = os.environ["ROOT_NODE"]
    access_token = os.environ["ACCESS_TOKEN"]
    hostname = os.environ["GEN3_HOSTNAME"]
    input_data = os.environ["INPUT_DATA"]

    gql = GuppyGQL(node=node, hostname="https://{}".format(hostname), access_token=access_token)
    case_ids = gql.execute(filters=input_data)

    sys.stderr.write(str(case_ids))

    dictionary_url = os.environ["DICTIONARY_URL"]

    with open("/peregrine-creds.json") as pelican_creds_file:
        peregrine_creds = json.load(pelican_creds_file)

    DB_URL = "jdbc:postgresql://{}/{}".format(
        peregrine_creds["db_host"], peregrine_creds["db_database"]
    )
    DB_USER = peregrine_creds["db_username"]
    DB_PASS = peregrine_creds["db_password"]

    dictionary, model = init_dictionary(url=dictionary_url)

    node_tables, edge_tables = get_tables(model)
    dd_tables = (node_tables, edge_tables)
    traverse_order = get_all_paths_dfs(model, node)

    # avro_schema = AvroSchema.from_dictionary(dictionary.schema)
    # schema = avro_schema.avro_schema
    # metadata = avro_schema.get_ontology_references()

    conf = (
        SparkConf()
            .set("spark.jars", os.environ["POSTGRES_JAR_PATH"])
            .set("spark.driver.memory", "10g")
            .set("spark.executor.memory", "10g")
            .setAppName("pelican")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as avro_output:
        with PFBWriter(avro_output) as pfb_file:
            _from_dict(pfb_file, dictionary_url)
            filename = pfb_file.name

    with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as avro_output:
        with PFBReader(filename) as reader:
            with PFBWriter(avro_output) as pfb_file:
                pfb_file.copy_schema(reader)
                pfb_file.write()
                fname = pfb_file.name

    with open(fname, "a+b") as avro_output:
        with PFBReader(filename) as reader:
            with PFBWriter(avro_output) as pfb_file:
                pfb_file.copy_schema(reader)
                export_avro(
                    spark,
                    pfb_file,
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

    avro_filename = "{}.avro".format(datetime.now().strftime('export_%Y-%m-%dT%H:%M:%S'))
    s3file = s3upload_file(
        pelican_creds["manifest_bucket_name"],
        avro_filename,
        pelican_creds["aws_access_key_id"],
        pelican_creds["aws_secret_access_key"],
        fname
    )

    print("[out] {}".format(s3file))
