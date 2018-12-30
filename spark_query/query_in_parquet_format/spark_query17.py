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

fun1 = lambda x, y: x * (1 - y)


