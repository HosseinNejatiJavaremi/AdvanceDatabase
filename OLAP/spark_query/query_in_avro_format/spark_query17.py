from pyspark import SparkContext
from pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query17")
sqlContext = SQLContext(sc)

lineitem = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/lineitem.avro")

part = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/part.avro")

from pyspark.sql import functions as F

fun1 = lambda x: x * 0.2

part_lineitem_filter = part.filter((part.P_BRAND == "Brand#23") & (part.P_CONTAINER == "JUMBO JAR")) \
    .select(part.P_PARTKEY) \
    .join(lineitem, part.P_PARTKEY == lineitem.L_PARTKEY, "left_outer")

query17 = part_lineitem_filter.groupBy(part_lineitem_filter.P_PARTKEY) \
    .agg(fun1(F.avg(part_lineitem_filter.L_QUANTITY)).alias("avg_quantity"))

query17 = query17.select(query17.P_PARTKEY.alias("key"), query17.avg_quantity)

query17 = query17.join(part_lineitem_filter, query17.key == part_lineitem_filter.P_PARTKEY) \
    .filter(part_lineitem_filter.L_QUANTITY < query17.avg_quantity) \
    .agg(F.sum(part_lineitem_filter.L_EXTENDEDPRICE) / 7.0)
query17.show()
