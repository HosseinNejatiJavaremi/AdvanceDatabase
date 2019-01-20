from pyspark import SparkContext
from pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query14")
sqlContext = SQLContext(sc)

customer = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/customer.avro")

lineitem = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/lineitem.avro")

nation = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/nation.avro")

orders = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/orders.avro")

part = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/part.avro")

partsupp = spark.read.format("com.databricks.spark.avro") \
    .load("hdfs://namenode:8020/hossein-avro-data/partsupp.avro")

region = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/region.avro")

supplier = spark.read.format("com.databricks.spark.avro") \
    .load("hdfs://namenode:8020/hossein-avro-data/supplier.avro")

from pyspark.sql import functions as F

fun1 = lambda x, y: x * (1 - y)

query14 = part.join(lineitem, (lineitem.L_PARTKEY == part.P_PARTKEY) &
                    (lineitem.L_SHIPDATE >= "1997-01-01") & (lineitem.L_SHIPDATE < "1997-02-01")) \
    .select(part.P_PARTKEY, fun1(lineitem.L_EXTENDEDPRICE, lineitem.L_DISCOUNT).alias("value"))

query14 = query14.agg(F.sum(F.when(part.P_PARTKEY.startswith("PROMO"),
                                   query14.value).otherwise(0)) * 100 / F.sum(query14.value))
query14.show()
