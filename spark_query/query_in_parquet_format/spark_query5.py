from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query5")
sqlContext = SQLContext(sc)

orders = spark.read.parquet("hdfs://namenode:8020/hossein-parquet-data/orders.parquet")
lineitem = spark.read.parquet("hdfs://namenode:8020/hossein-parquet-data/lineitem.parquet")
supplier = spark.read.parquet("hdfs://namenode:8020/hossein-parquet-data/supplier.parquet")
nation = spark.read.parquet("hdfs://namenode:8020/hossein-parquet-data/nation.parquet")
region = spark.read.parquet("hdfs://namenode:8020/hossein-parquet-data/region.parquet")
customer = spark.read.parquet("hdfs://namenode:8020/hossein-parquet-data/customer.parquet")

from pyspark.sql import functions as F

fun1 = lambda x, y: x * (1 - y)
orders_filter = orders.filter((orders.O_ORDERDATE < "1998-01-01") & (orders.O_ORDERDATE >= "1997-01-01"))

query5 = region.filter(region.R_NAME == "ASIA") \
    .join(nation, region.R_REGIONKEY == nation.N_REGIONKEY) \
    .join(supplier, nation.N_NATIONKEY == supplier.S_NATIONKEY) \
    .join(lineitem, supplier.S_SUPPKEY == lineitem.L_SUPPKEY) \
    .select(nation.N_NAME, lineitem.L_EXTENDEDPRICE, lineitem.L_DISCOUNT,
            lineitem.L_ORDERKEY, supplier.S_NATIONKEY) \
    .join(orders_filter, lineitem.L_ORDERKEY == orders_filter.O_ORDERKEY) \
    .join(customer, (orders_filter.O_CUSTKEY == customer.C_CUSTKEY)
          & (supplier.S_NATIONKEY == customer.C_NATIONKEY)) \
    .select(nation.N_NAME, fun1(lineitem.L_EXTENDEDPRICE, lineitem.L_DISCOUNT).alias("value")) \
    .groupBy(nation.N_NAME) \
    .agg(F.sum("value").alias("revenue"))
query5 = query5.sort(query5.revenue.desc())
