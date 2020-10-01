import json
import os
import tempfile
import sqlalchemy

from pyspark import SparkConf
from pyspark.sql import SparkSession

from pelican.jobs import import_pfb_job
from pelican.dictionary import init_dictionary, DataDictionaryTraversal
from pelican.s3 import download_file

if __name__ == "__main__":
    access_token = os.environ["ACCESS_TOKEN"]
    hostname = os.environ["GEN3_HOSTNAME"]
    input_data = os.environ["INPUT_DATA"]

    input_data_json = json.loads(input_data)

    dictionary_url = os.environ["DICTIONARY_URL"]

    with open("/sheepdog-creds.json") as pelican_creds_file:
        sheepdog_creds = json.load(pelican_creds_file)

    DB_URL = "jdbc:postgresql://{}/{}".format(
        sheepdog_creds["db_host"], sheepdog_creds["db_database"]
    )
    DB_USER = sheepdog_creds["db_username"]
    DB_PASS = sheepdog_creds["db_password"]

    # create a database in the name that was passed through
    engine = "postgres://{user}@{host}/postgres".format(user=DB_USER, host=sheepdog_creds["db_host"])
    conn = engine.connect()
    conn.execute("commit")

    print("_______________________________________")
    print("we are creating a new database named newtest0")
    print("_______________________________________")

    conn.execute("create database newtest0")
    conn.execute("grant all on database newtest- to sheepdog")
    conn.close()
    # gen3 psql sheepdog -c "CREATE DATABASE TEST;"
    # gen3 psql sheepdog -c "GRANT ALL ON DATABASE TEST TO sheepdog

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

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        download_file(input_data_json["url"], fileobj=tmp)
        name = tmp.name

    import_pfb_job(spark, name, ddt, DB_URL, DB_USER, DB_PASS)
