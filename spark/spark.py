import os

from pyspark import SparkConf, SparkContext


def init_spark_context():
    # .set("spark.jars.packages", "org.apache.spark:spark-avro_2.11:2.4.0") \
    conf = SparkConf() \
        .set("spark.jars", os.environ['POSTGRES_JAR_PATH']) \
        .set("spark.driver.memory", "2g") \
        .set("spark.executor.memory", "2g") \
        .setAppName('pelican')
    sc = SparkContext(conf=conf)
    # sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])
    return sc
