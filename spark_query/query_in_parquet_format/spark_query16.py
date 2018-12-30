from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query16")
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

part_filter = part.filter((part.P_BRAND != "Brand#45") & ~(part.P_TYPE.like("MEDIUM POLISHED")) &
              (part.P_SIZE.isin([7, 13, 21, 28, 33, 49, 50, 1]))) \
            .select(part.P_PARTKEY, part.P_BRAND, part.P_TYPE, part.P_SIZE)


query16 = supplier.filter(~supplier.S_COMMENT.like("*Customer*Complaints*"))\
      .select(supplier.S_SUPPKEY)\
      .join(partsupp, supplier.S_SUPPKEY == partsupp.PS_SUPPKEY) \
      .select(partsupp.PS_PARTKEY, partsupp.PS_SUPPKEY) \
      .join(part_filter,partsupp.PS_PARTKEY == part_filter.P_PARTKEY)\
      .groupBy(part_filter.P_BRAND, part_filter.P_TYPE, part_filter.P_SIZE)\
      .agg(F.countDistinct(partsupp.PS_SUPPKEY).alias("supplier_count"))

query16 = query16.sort(query16.supplier_count.desc(), query16.P_BRAND, query16.P_TYPE, query16.P_SIZE)

query16.show()
