import os

from pyspark import SparkConf, SparkContext


def init_spark_context():
    conf = (
        SparkConf()
        .set("spark.jars", os.environ["POSTGRES_JAR_PATH"])
        .set("spark.driver.memory", "2g")
        .set("spark.executor.memory", "2g")
        .setAppName("pelican")
    )
    sc = SparkContext(conf=conf)
    return sc
