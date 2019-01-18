from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="query12")
sqlContext = SQLContext(sc)

lineitem = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/lineitem.parquet")
orders = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/orders.parquet")

from pyspark.sql import functions as F

query12 = lineitem.filter(((lineitem.L_SHIPMODE == "AIR") | (lineitem.L_SHIPMODE == "RAIL")) &
                           (lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE) &
                           (lineitem.L_SHIPDATE < lineitem.L_COMMITDATE) &
                           (lineitem.L_RECEIPTDATE >= "1997-01-01") &
                           (lineitem.L_RECEIPTDATE < "1998-01-01")) \
    .join(orders, lineitem.L_ORDERKEY == orders.O_ORDERKEY) \
    .select(lineitem.L_SHIPMODE, orders.O_ORDERPRIORITY) \
    .groupBy(lineitem.L_SHIPMODE) \
    .agg(F.sum(F.when((orders.O_ORDERPRIORITY == "1-URGENT") |
                      (orders.O_ORDERPRIORITY == "2-HIGH"), 1).otherwise(0).alias("sum_highorderpriority")),
         F.sum(F.when((orders.O_ORDERPRIORITY != "1-URGENT") &
                      (orders.O_ORDERPRIORITY != "2-HIGH"), 1).otherwise(0)).alias("sum_loworderpriority")) \
    .sort(lineitem.L_SHIPMODE)
query12.show()
