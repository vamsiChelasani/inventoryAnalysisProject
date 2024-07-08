from pyspark.sql import SparkSession
from lib.configReader import get_spark_config

def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
                 .config(conf=get_spark_config(env)) \
                 .master("local[2]") \
                 .getOrCreate()
    else:
        return SparkSession.builder \
           .config(conf=get_spark_config(env)) \
           .enableHiveSupport() \
           .getOrCreate()