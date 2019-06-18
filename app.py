import os
import sys

from avro_utils.avro_schema import AvroSchema
from pyspark.sql import SparkSession

from util.avro.export import export_avro
from util.dictionary import get_all_paths
from util.dictionary import init_dictionary, get_tables
from util.graphql.guppy_gql import GuppyGQL
from util.s3 import s3upload_file
from util.spark import init_spark_context

from datetime import datetime


if __name__ == "__main__":
    gql = GuppyGQL(hostname="https://{}".format(os.environ["GEN3_HOSTNAME"]))
    case_ids = gql.execute(filters=os.environ["INPUT_DATA"])

    sys.stderr.write(str(case_ids))

    sc = init_spark_context()

    DICTIONARY_URL = os.environ["DICTIONARY_URL"]
    DB_URL = "jdbc:postgresql://{}/{}".format(
        os.environ["DB_HOST"], os.environ["DB_DATABASE"]
    )
    DB_USER = os.environ["DB_USERNAME"]
    DB_PASS = os.environ["DB_PASSWORD"]

    dictionary_url = DICTIONARY_URL
    dictionary, model = init_dictionary(url=dictionary_url)

    avro_schema = AvroSchema.from_dictionary(dictionary.schema)
    schema = avro_schema.avro_schema
    metadata = avro_schema.get_ontology_references()

    node_tables, edge_tables = get_tables(model)
    dd_tables = (node_tables, edge_tables)

    spark = SparkSession.builder.getOrCreate()

    traverse_order = get_all_paths(model, "case")

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
    )

    s3file = s3upload_file(
        os.environ["BUCKET_NAME"],
        "{}.avro".format(datetime.now().strftime('export_%Y-%m-%dT%H:%M:%S')),
        os.environ["S3_KEY"],
        os.environ["S3_SECRET"],
        avro_filename,
    )

    print("[out] {}".format(s3file))
