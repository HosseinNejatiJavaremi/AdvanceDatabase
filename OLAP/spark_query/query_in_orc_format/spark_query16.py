from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="query16")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')
part = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/part.orc")
partsupp = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/partsupp.orc")
supplier = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/supplier.orc")

from pyspark.sql import functions as F

fun1 = lambda x, y: x * (1 - y)

part_filter = part.filter((part.P_BRAND != "Brand#45") & ~(part.P_TYPE.like("MEDIUM POLISHED%")) &
                          (part.P_SIZE.isin([7, 13, 21, 28, 33, 49, 50, 1]))) \
    .select(part.P_PARTKEY, part.P_BRAND, part.P_TYPE, part.P_SIZE)

query16 = supplier.filter(~supplier.S_COMMENT.like("*Customer*Complaints*")) \
    .select(supplier.S_SUPPKEY) \
    .join(partsupp, supplier.S_SUPPKEY == partsupp.PS_SUPPKEY) \
    .select(partsupp.PS_PARTKEY, partsupp.PS_SUPPKEY) \
    .join(part_filter, partsupp.PS_PARTKEY == part_filter.P_PARTKEY) \
    .groupBy(part_filter.P_BRAND, part_filter.P_TYPE, part_filter.P_SIZE) \
    .agg(F.countDistinct(partsupp.PS_SUPPKEY).alias("supplier_count"))

query16 = query16.sort(query16.supplier_count.desc(), query16.P_BRAND, query16.P_TYPE, query16.P_SIZE)

query16.show()
