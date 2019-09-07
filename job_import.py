import json
import os
import tempfile

from pyspark import SparkConf
from pyspark.sql import SparkSession

from pelican.avro.imp import import_avro
from pelican.dictionary import init_dictionary, DataDictionaryTraversal
from pelican.s3 import download_file

if __name__ == "__main__":
    node = os.environ["ROOT_NODE"]
    access_token = os.environ["ACCESS_TOKEN"]
    hostname = os.environ["GEN3_HOSTNAME"]
    input_data = os.environ["INPUT_DATA"]

    input_data_json = json.loads(input_data)

    dictionary_url = os.environ["DICTIONARY_URL"]

    with open("/peregrine-creds.json") as pelican_creds_file:
        peregrine_creds = json.load(pelican_creds_file)

    DB_URL = "jdbc:postgresql://{}/{}".format(
        peregrine_creds["db_host"], peregrine_creds["db_database"]
    )
    DB_USER = peregrine_creds["db_username"]
    DB_PASS = peregrine_creds["db_password"]

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

    with tempfile.TemporaryFile() as tmp:
        download_file(input_data_json["url"], fileobj=tmp)
        name = tmp.name

    import_avro(spark, name, ddt, DB_URL, DB_USER, DB_PASS)
