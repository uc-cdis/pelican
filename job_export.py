import json
import os
import tempfile
from datetime import datetime

from pfb.importers.gen3dict import _from_dict
from pfb.reader import PFBReader
from pfb.writer import PFBWriter
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pelican.dictionary import init_dictionary, DataDictionaryTraversal
from pelican.graphql.guppy_gql import GuppyGQL
from pelican.jobs import export_pfb_job
from pelican.s3 import s3upload_file

if __name__ == "__main__":
    node = os.environ["ROOT_NODE"]
    access_token = os.environ["ACCESS_TOKEN"]
    hostname = os.environ["GEN3_HOSTNAME"]
    input_data = os.environ["INPUT_DATA"]

    gql = GuppyGQL(node=node, hostname="https://{}".format(hostname), access_token=access_token)
    case_ids = gql.execute(filters=input_data)

    with open("/peregrine-creds.json") as pelican_creds_file:
        peregrine_creds = json.load(pelican_creds_file)

    DB_URL = "jdbc:postgresql://{}/{}".format(
        peregrine_creds["db_host"], peregrine_creds["db_database"]
    )
    DB_USER = peregrine_creds["db_username"]
    DB_PASS = peregrine_creds["db_password"]

    dictionary_url = os.environ["DICTIONARY_URL"]
    dictionary, model = init_dictionary(url=dictionary_url)
    ddt = DataDictionaryTraversal(model)

    conf = (
        SparkConf()
            .set("spark.jars", os.environ["POSTGRES_JAR_PATH"])
            .set("spark.driver.memory", "10g")
            .set("spark.executor.memory", "10g")
            .setAppName("pelican")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    db = spark.read.format("jdbc").options(
        url=DB_URL, user=DB_USER, password=DB_PASS, driver="org.postgresql.Driver"
    )

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
                export_pfb_job(
                    db,
                    pfb_file,
                    ddt,
                    case_ids,
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
