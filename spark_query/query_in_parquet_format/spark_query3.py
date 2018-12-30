from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query3")
sqlContext = SQLContext(sc)

customer = spark.read.parquet("hdfs://namenode:8020/hossein-parquet-data/customer.parquet")
orders = spark.read.parquet("hdfs://namenode:8020/hossein-parquet-data/orders.parquet")
lineitem = spark.read.parquet("hdfs://namenode:8020/hossein-parquet-data/lineitem.parquet")

from pyspark.sql import functions as F

fun1 = lambda x, y: x * (1 - y)
customer_filter = customer.filter(customer.C_MKTSEGMENT == "HOUSEHOLD")
orders_filter = orders.filter(orders.O_ORDERDATE < "1995-03-15")
lineitem_filter = lineitem.filter(lineitem.L_SHIPDATE > "1995-03-15")

query3 = customer_filter.join(orders_filter, customer_filter.C_CUSTKEY == orders_filter.O_CUSTKEY) \
    .select(orders_filter.O_ORDERKEY, orders_filter.O_ORDERDATE, orders_filter.O_SHIPPRIORITY) \
    .join(lineitem_filter, orders_filter.O_ORDERKEY == lineitem_filter.L_ORDERKEY) \
    .select(lineitem_filter.L_ORDERKEY,
            fun1(lineitem_filter.L_EXTENDEDPRICE, lineitem_filter.L_DISCOUNT).alias("volume"),
            orders_filter.O_ORDERDATE, orders_filter.O_SHIPPRIORITY) \
    .groupBy(lineitem_filter.L_ORDERKEY, orders_filter.O_ORDERDATE, orders_filter.O_SHIPPRIORITY) \
    .agg(F.sum("volume").alias("revenue"))
query3 = query3.sort(query3.revenue.desc(), orders_filter.O_ORDERDATE) \
    .limit(10)

query3.show()
