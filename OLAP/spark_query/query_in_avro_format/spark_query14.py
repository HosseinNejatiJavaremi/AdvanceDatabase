from pyspark import SparkContext
from pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query14")
sqlContext = SQLContext(sc)

lineitem = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/lineitem.avro")

part = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/part.avro")

from pyspark.sql import functions as F

fun1 = lambda x, y: x * (1 - y)

query14 = part.join(lineitem, (lineitem.L_PARTKEY == part.P_PARTKEY) &
                    (lineitem.L_SHIPDATE >= "1997-01-01") & (lineitem.L_SHIPDATE < "1997-02-01")) \
    .select(part.P_PARTKEY, fun1(lineitem.L_EXTENDEDPRICE, lineitem.L_DISCOUNT).alias("value"))

query14 = query14.agg(F.sum(F.when(part.P_PARTKEY.startswith("PROMO"),
                                   query14.value).otherwise(0)) * 100 / F.sum(query14.value))
query14.show()
