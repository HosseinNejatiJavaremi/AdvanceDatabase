from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query10")
sqlContext = SQLContext(sc)

customer = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/customer.parquet")
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/lineitem.parquet")
nation = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/nation.parquet")
orders = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/orders.parquet")

from pyspark.sql import functions as F

fun1 = lambda x, y: x * (1 - y)

lineitem_filter = lineitem.filter(lineitem.L_RETURNFLAG == "R")

query10 = orders.filter((orders.O_ORDERDATE < "1995-04-01") & (orders.O_ORDERDATE >= "1995-01-01")) \
        .join(customer, orders.O_CUSTKEY == customer.C_CUSTKEY) \
        .join(nation, customer.C_NATIONKEY == nation.N_NATIONKEY) \
        .join(lineitem_filter, orders.O_ORDERKEY == lineitem_filter.L_ORDERKEY) \
        .select(customer.C_CUSTKEY, customer.C_NAME,
                fun1(lineitem.L_EXTENDEDPRICE, lineitem.L_DISCOUNT).alias("volume"),
                customer.C_ACCTBAL, nation.N_NAME, customer.C_ADDRESS, customer.C_PHONE, customer.C_COMMENT) \
        .groupBy(customer.C_CUSTKEY, customer.C_NAME, customer.C_ACCTBAL, customer.C_PHONE,
                 nation.N_NAME, customer.C_ADDRESS, customer.C_COMMENT) \
        .agg(F.sum("volume").alias("revenue"))
query10 = query10.sort(query10.revenue.desc()) \
        .limit(20)
