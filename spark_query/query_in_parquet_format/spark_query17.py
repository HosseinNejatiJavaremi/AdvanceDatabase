from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query17")
sqlContext = SQLContext(sc)

supplier = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/supplier.parquet")
customer = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/customer.parquet")
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/lineitem.parquet")
nation = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/nation.parquet")
orders = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/orders.parquet")
part = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/part.parquet")
partsupp = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/partsupp.parquet")
region = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/region.parquet")

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
