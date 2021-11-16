import json
import os
import tempfile
import sqlalchemy
import requests

from pyspark import SparkConf
from pyspark.sql import SparkSession

from pelican.jobs import import_pfb_job
from pelican.dictionary import init_dictionary, DataDictionaryTraversal
from pelican.s3 import download_file

from sqlalchemy.sql import text

if __name__ == "__main__":
    access_token = os.environ["ACCESS_TOKEN"]
    hostname = os.environ["GEN3_HOSTNAME"]
    input_data = os.environ["INPUT_DATA"]

    input_data_json = json.loads(input_data)

    dictionary_url = os.environ["DICTIONARY_URL"]

    with open("/sheepdog-creds.json") as pelican_creds_file:
        sheepdog_creds = json.load(pelican_creds_file)

    if "guid" in input_data_json:
        print("a guid was supplied to the job")
        print("we are getting a signed url for the given guid")

        host = "http://revproxy-service"

        auth_headers = {"Authorization": "Bearer " + access_token}

        api_url = (
            host + "/user/data/download/" + input_data_json["guid"] + "?protocol=s3"
        )

        signed_request = requests.get(api_url, headers=auth_headers)

        signed_url = signed_request.json()
        print("the signed url is ", signed_url["url"])
        input_data_json["url"] = signed_url["url"]

    # DB_URL = "jdbc:postgresql://{}/{}".format(
    #     sheepdog_creds["db_host"], sheepdog_creds["db_database"]
    # )
    DB_USER = sheepdog_creds["db_username"]
    DB_PASS = sheepdog_creds["db_password"]

    NEW_DB_NAME = input_data_json["db"]

    # create a database in the name that was passed through
    engine = sqlalchemy.create_engine(
        "postgresql://{user}:{password}@{host}/postgres".format(
            user=DB_USER, password=DB_PASS, host=sheepdog_creds["db_host"]
        )
    )
    conn = engine.connect()
    conn.execute("commit")

    print("we are creating a new database named ", NEW_DB_NAME)

    create_db_command = text("create database :db")
    print("This is the db create command: ", create_db_command)

    grant_db_access = text("grant all on database :db to sheepdog with grant option")
    print("This is the db access command: ", grant_db_access)

    try:
        conn.execute(create_db_command, db=NEW_DB_NAME)
        conn.execute(grant_db_access, db=NEW_DB_NAME)
    except Exception:
        print("Unable to create database")
        raise Exception

    conn.close()

    DB_URL = "jdbc:postgresql://{}/{}".format(sheepdog_creds["db_host"], NEW_DB_NAME)

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
