from pyspark import SparkContext
from pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query11")
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

fun1 = lambda x, y: x * y
fun2 = lambda x: x * 0.000001

iran = nation.filter(nation.N_NAME == "IRAN") \
    .join(supplier, nation.N_NATIONKEY == supplier.S_NATIONKEY) \
    .select(supplier.S_SUPPKEY) \
    .join(partsupp, supplier.S_SUPPKEY == partsupp.PS_SUPPKEY) \
    .select(partsupp.PS_PARTKEY, fun1(partsupp.PS_SUPPLYCOST, partsupp.PS_AVAILQTY).alias("value"))

total = iran.agg(F.sum("value").alias("total_value"))

query11 = iran.groupBy(partsupp.PS_PARTKEY).agg(F.sum("value").alias("group_value"))

query11 = query11.join(total, query11.group_value > fun2(total.total_value)) \
    .sort(query11.group_value.desc())
query11.show()
