from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query4")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')
lineitem = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/lineitem.orc")
orders = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/orders.orc")

from pyspark.sql import functions as F

orders_filter = orders.filter((orders.O_ORDERDATE >= "1997-01-01") & (orders.O_ORDERDATE < "1997-04-01"))
lineitem_filter = lineitem.filter(lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE) \
    .select(lineitem.L_ORDERKEY).distinct()

query4 = lineitem_filter.join(orders_filter, lineitem.L_ORDERKEY == orders_filter.O_ORDERKEY) \
    .groupBy(orders_filter.O_ORDERPRIORITY) \
    .agg(F.count(orders_filter.O_ORDERPRIORITY)) \
    .sort(orders_filter.O_ORDERPRIORITY)

query4.show()