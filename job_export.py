import json
import os
import tempfile
import hashlib

from datetime import datetime

from pfb.importers.gen3dict import _from_dict
from pfb.reader import PFBReader
from pfb.writer import PFBWriter
from pyspark import SparkConf
from pyspark.sql import SparkSession

from gen3.index import Gen3Index
from gen3.auth import Gen3Auth

from pelican.dictionary import init_dictionary, DataDictionaryTraversal
from pelican.graphql.guppy_gql import GuppyGQL
from pelican.jobs import export_pfb_job
from pelican.s3 import s3upload_file
from pelican.indexd import indexd_submit

if __name__ == "__main__":
    node = os.environ["ROOT_NODE"]
    access_token = os.environ["ACCESS_TOKEN"]
    input_data = os.environ["INPUT_DATA"]

    gql = GuppyGQL(node=node, hostname="http://revproxy-service", access_token=access_token)
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

    if "gtex" in dictionary_url:
        extra_nodes = ["reference_file", "reference_file_index"]
    else:
        extra_nodes = None

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
                    node,
                    extra_nodes,
                    True  # include upward nodes: project, program etc
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

    # calculate md5 sum
    md5_sum = hashlib.md5()
    chunk_size = 8192
    with open(fname, "rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            md5_sum.update(data)

    md5_digest = md5_sum.hexdigest()

    hostname = os.environ["GEN3_HOSTNAME"]

    COMMONS = "https://" + hostname + "/"

    # try sending to indexd
    # auth = Gen3Auth(COMMONS, refresh_file=access_token)
    # index = Gen3Index(COMMONS, auth_provider=auth)

    with open("/indexd-creds.json") as indexd_creds_file:
        indexd_creds = json.load(indexd_creds_file)

    indexd_record = indexd_submit(
        COMMONS,
        indexd_creds["user_db"]["gdcapi"],
        avro_filename,
        os.stat(fname).st_size,
        [s3file],
        {"md5": str(md5_digest)}
    )    

    # if not index.is_healthy():
    #     print(f"uh oh! The indexing service is not healthy in the commons {COMMONS}")

    # print("trying to create new indexed file object record:\n")
    # try:
    #     response = index.create_record(
    #         filename = avro_filename,
    #         hashes={"md5": str(md5_digest)}, 
    #         urls = [s3file],
    #         size=os.stat(fname).st_size
    #     )
    # except Exception as exc:
    #     print(
    #         "\nERROR ocurred when trying to create the record, you probably don't have access."
    #     )

    # send s3 link and information to indexd to create guid and send it back

    print("[out] {}".format(indexd_record["did"]))
