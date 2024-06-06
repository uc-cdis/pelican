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

from gdcdatamodel.models.submission import Base

if __name__ == "__main__":
    access_token = os.environ["ACCESS_TOKEN"]
    hostname = os.environ["GEN3_HOSTNAME"]
    input_data = os.environ["INPUT_DATA"]

    input_data_json = json.loads(input_data)

    dictionary_url = os.environ["DICTIONARY_URL"]

    with open("/sheepdog-creds.json") as sheepdog_creds_file:
        sheepdog_creds = json.load(sheepdog_creds_file)

    with open("/admin-servers.json") as dbfarm_file:
        servers = json.load(dbfarm_file)

    # we look through the servers in the dbfarm to find the sheepdog db server
    for s in servers:
        if servers[s]["db_host"] == sheepdog_creds["db_host"]:
            db_server = servers[s]

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

    # setup DB engine for postgres user
    # this is needed as only the postgres user can create a new database and the sheepdog user cannot
    DB_USER = db_server["db_username"]
    DB_PASS = db_server["db_password"]

    NEW_DB_NAME = input_data_json["db"]

    # create a database in the name that was passed through
    engine = sqlalchemy.create_engine(
        "postgresql://{user}:{password}@{host}/postgres".format(
            user=DB_USER, password=DB_PASS, host=db_server["db_host"]
        )
    )
    conn = engine.connect()
    conn.execute("commit")

    print("we are creating a new database named ", NEW_DB_NAME)

    create_str = "CREATE DATABASE {db}"
    create_db_command = create_str.format(db=NEW_DB_NAME)
    print("This is the db create command: ", create_db_command)

    grant_str = "GRANT ALL ON DATABASE {db} TO {username} WITH GRANT OPTION"
    grant_db_access = grant_str.format(
        db=NEW_DB_NAME, username=sheepdog_creds["db_username"]
    )
    print("This is the db access command: ", grant_db_access)
    try:
        conn.execute(create_db_command)
        conn.execute("commit")

        conn.execute(grant_db_access)
        conn.execute("commit")
    except Exception as e:
        print("Unable to create database... error: ", e)
        raise Exception

    # close db connection for root user
    conn.close()

    # setup the transaction tables for the db
    print("Setting up transaction tables for new sheepdog db")
    PORT = "5432"
    engine = sqlalchemy.create_engine(
        "postgresql://{user}:{password}@{host}:{port}/{database}".format(
            user=DB_USER,
            host=db_server["db_host"],
            port=PORT,
            password=DB_PASS,
            database=NEW_DB_NAME,
        )
    )

    # create db transaction tables
    Base.metadata.create_all(engine)

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
