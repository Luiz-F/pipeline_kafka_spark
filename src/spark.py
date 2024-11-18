import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,FloatType,IntegerType,StringType,TimestampType
from pyspark.sql.functions import from_json,col, to_json
from struct import Struct


spark = SparkSession \
        .builder \
        .appName("Spark-Kafka-Streaming") \
        .master("spark://172.20.0.5:7077") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.driver.host", "localhost")\
        .config("spark.jars", os.getcwd() + "/jars/spark-sql-kafka-0-10_2.12-3.0.0.jar" + "," +
                              os.getcwd() + "/jars/kafka-clients-3.0.0.jar" + "," +
                              os.getcwd() + "/jars/spark-streaming-kafka-0-10_2.12-3.0.0.jar" + "," +
                              os.getcwd() + "/jars/commons-pool2-2.8.0.jar" + "," +
                              os.getcwd() + "/jars/spark-token-provider-kafka-0-10_2.12-3.0.0.jar") \
        .getOrCreate()

df = spark.read.json('dados_hoteis.json')

df.show()

