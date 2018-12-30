from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query15")
sqlContext = SQLContext(sc)

lineitem = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/lineitem.parquet")
supplier = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/supplier.parquet")

from pyspark.sql import functions as F

fun1 = lambda x, y: x * (1 - y)

revenue = lineitem.filter((lineitem.L_SHIPDATE >= "1997-05-01") &
                          (lineitem.L_SHIPDATE < "1997-08-01")) \
    .select(lineitem.L_SUPPKEY, fun1(lineitem.L_EXTENDEDPRICE, lineitem.L_DISCOUNT).alias("value")) \
    .groupBy(lineitem.L_SUPPKEY).agg(F.sum('value').alias("total"))

query15 = revenue.agg(F.max(revenue.total).alias("max_total"))
query15 = query15.join(revenue, query15.max_total == revenue.total) \
    .join(supplier, lineitem.L_SUPPKEY == supplier.S_SUPPKEY) \
    .select(supplier.S_SUPPKEY, supplier.S_NAME, supplier.S_ADDRESS, supplier.S_PHONE, revenue.total) \
    .sort(supplier.S_SUPPKEY)
